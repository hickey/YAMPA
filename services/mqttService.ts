import { Packet } from '../types';
import { PacketDecoder, RawPacketData } from './packetDecoder';
import mqtt, { MqttClient, IClientOptions } from 'mqtt';

type PacketCallback = (packet: Packet) => void;
type StatusCallback = (status: MqttConnectionStatus) => void;

export type MqttConnectionStatus = 'connected' | 'disconnected' | 'connecting' | 'error';

export interface MqttConfig {
  brokerUrl: string;       // e.g., ws://broker:9001/mqtt
  topicPattern: string;    // e.g., meshcore/+/+/packets
  username?: string;
  password?: string;
}

export class MqttService {
  private client: MqttClient | null = null;
  private callbacks: PacketCallback[] = [];
  private statusCallbacks: StatusCallback[] = [];
  private config: MqttConfig;
  private isPaused: boolean = false;

  constructor(config: MqttConfig) {
    this.config = config;
  }

  public subscribe(callback: PacketCallback): () => void {
    this.callbacks.push(callback);
    return () => {
      this.callbacks = this.callbacks.filter((cb) => cb !== callback);
    };
  }

  public subscribeStatus(callback: StatusCallback): () => void {
    this.statusCallbacks.push(callback);
    callback(this.getCurrentStatus());
    return () => {
      this.statusCallbacks = this.statusCallbacks.filter((cb) => cb !== callback);
    };
  }

  private emitStatus(status: MqttConnectionStatus) {
    this.statusCallbacks.forEach(cb => cb(status));
  }

  private getCurrentStatus(): MqttConnectionStatus {
    if (!this.client) return 'disconnected';
    if (this.client.connected) return 'connected';
    return 'connecting';
  }

  public updateConfig(config: MqttConfig) {
    this.config = config;
  }

  public connect() {
    if (this.client) return;
    if (this.isPaused) return;

    this.emitStatus('connecting');

    const options: IClientOptions = {
      reconnectPeriod: 3000,
    };

    if (this.config.username) {
      options.username = this.config.username;
    }
    if (this.config.password) {
      options.password = this.config.password;
    }

    this.client = mqtt.connect(this.config.brokerUrl, options);

    this.client.on('connect', () => {
      this.emitStatus('connected');
      this.client?.subscribe(this.config.topicPattern, (err) => {
        if (err) {
          console.error('MQTT subscribe error:', err);
        }
      });
    });

    this.client.on('message', (_topic: string, payload: Buffer) => {
      if (this.isPaused) return;
      try {
        const message = JSON.parse(payload.toString());
        const rawData = this.transformToRawPacketData(message);
        PacketDecoder.decodeRawPacket(rawData).then(decodedPacket => {
          this.emit(decodedPacket);
        });
      } catch (e) {
        console.error('Failed to parse MQTT message:', e);
      }
    });

    this.client.on('error', (err) => {
      console.error('MQTT error:', err);
      this.emitStatus('error');
    });

    this.client.on('close', () => {
      this.emitStatus('disconnected');
    });

    this.client.on('reconnect', () => {
      this.emitStatus('connecting');
    });
  }

  public disconnect() {
    if (this.client) {
      this.client.end(true);
      this.client = null;
    }
    this.emitStatus('disconnected');
  }

  public pause() {
    this.isPaused = true;
    this.disconnect();
  }

  public resume() {
    this.isPaused = false;
    this.connect();
  }

  /**
   * Transform MQTT payload to the RawPacketData format expected by PacketDecoder.
   *
   * MQTT payload example:
   * {
   *   "origin": "wt0f-observer",
   *   "origin_id": "617CF109...",
   *   "timestamp": "2026-06-25T21:29:16.300050",
   *   "type": "PACKET",
   *   "direction": "rx",
   *   "raw": "1141277DFA30EA...",
   *   "SNR": "12",
   *   "RSSI": "-60"
   * }
   */
  private transformToRawPacketData(message: any): RawPacketData {
    // Parse ISO timestamp to epoch seconds
    let ts: number;
    if (message.timestamp) {
      ts = new Date(message.timestamp).getTime() / 1000;
    } else {
      ts = Date.now() / 1000;
    }

    return {
      ts,
      raw_packet: {
        hex: message.raw || '',
      },
      radio: {
        rssi: message.RSSI ? parseInt(message.RSSI, 10) : 0,
        snr: message.SNR ? parseInt(message.SNR, 10) : 0,
      },
    };
  }

  private emit(packet: Packet) {
    this.callbacks.forEach((cb) => cb(packet));
  }
}
