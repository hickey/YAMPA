import { Packet } from '../types';
import { MOCK_PACKET_DATA } from '../constants';
import { PacketDecoder, RawPacketData } from './packetDecoder';
import { MqttService, MqttConfig, MqttConnectionStatus } from './mqttService';

type PacketCallback = (packet: Packet) => void;
type StatusCallback = (status: ConnectionStatus) => void;

export type ConnectionStatus = 'connected' | 'disconnected' | 'connecting' | 'error';
export type ConnectionMode = 'websocket' | 'mqtt' | 'simulation';

export class StreamService {
  private packets: RawPacketData[] = [];
  private callbacks: PacketCallback[] = [];
  private statusCallbacks: StatusCallback[] = [];

  private intervalId: number | null = null;
  private currentIndex: number = 0;
  private isPaused: boolean = false;

  // WebSocket state
  private ws: WebSocket | null = null;
  private wsUrl: string = 'ws://192.168.178.93:8080/ws';
  private reconnectTimeoutId: number | null = null;

  // MQTT state
  private mqttService: MqttService | null = null;
  private mqttConfig: MqttConfig = {
    brokerUrl: `ws://${import.meta.env.MQTT_HOST || 'localhost'}:${import.meta.env.MQTT_PORT || '8083'}/mqtt`,
    topicPattern: import.meta.env.MQTT_TOPIC || 'meshcore/#',
    username: import.meta.env.MQTT_USERNAME || undefined,
    password: import.meta.env.MQTT_PASSWORD || undefined,
  };

  // Mode — default to MQTT if MQTT_ENABLE is set
  private connectionMode: ConnectionMode = import.meta.env.MQTT_ENABLE ? 'mqtt' : 'websocket';

  constructor() {
    this.parseMockData();
  }

  private parseMockData() {
    this.packets = MOCK_PACKET_DATA.trim()
      .split('\n')
      .filter((line) => line.trim().length > 0)
      .map((line) => {
        try {
          const fullPacket = JSON.parse(line);
          return {
            ts: fullPacket.ts,
            raw_packet: fullPacket.raw_packet,
            radio: fullPacket.radio,
            routing: fullPacket.routing,
          } as RawPacketData;
        } catch (e) {
          console.error("Failed to parse packet line:", line);
          return null;
        }
      })
      .filter((p): p is RawPacketData => p !== null)
      .sort((a, b) => a.ts - b.ts);
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

  private emitStatus(status: ConnectionStatus) {
    this.statusCallbacks.forEach(cb => cb(status));
  }

  private getCurrentStatus(): ConnectionStatus {
    if (this.connectionMode === 'simulation') return 'connected';
    if (this.connectionMode === 'mqtt') {
      if (!this.mqttService) return 'disconnected';
      return 'disconnected'; // Will be updated via MQTT status callback
    }
    if (!this.ws) return 'disconnected';
    switch (this.ws.readyState) {
      case WebSocket.CONNECTING: return 'connecting';
      case WebSocket.OPEN: return 'connected';
      case WebSocket.CLOSING:
      case WebSocket.CLOSED: return 'disconnected';
      default: return 'disconnected';
    }
  }

  public getConnectionMode(): ConnectionMode {
    return this.connectionMode;
  }

  public setConnectionMode(mode: ConnectionMode) {
    if (this.connectionMode === mode) return;
    this.stop();
    this.connectionMode = mode;
    this.start();
  }

  // Legacy method for backward compatibility
  public setSimulationMode(enabled: boolean) {
    if (enabled) {
      this.setConnectionMode('simulation');
    } else {
      this.setConnectionMode('websocket');
    }
  }

  public setWsUrl(url: string) {
    this.wsUrl = url;
  }

  public setMqttConfig(config: Partial<MqttConfig>) {
    this.mqttConfig = { ...this.mqttConfig, ...config };
    if (this.mqttService) {
      this.mqttService.updateConfig(this.mqttConfig);
    }
  }

  public getMqttConfig(): MqttConfig {
    return { ...this.mqttConfig };
  }

  public start() {
    switch (this.connectionMode) {
      case 'simulation':
        this.startSimulation();
        break;
      case 'mqtt':
        this.connectMqtt();
        break;
      case 'websocket':
      default:
        this.connectWebSocket();
        break;
    }
  }

  public stop() {
    this.stopSimulation();
    this.closeWebSocket();
    this.disconnectMqtt();
  }

  public pause() {
    this.isPaused = true;
    switch (this.connectionMode) {
      case 'simulation':
        this.stopSimulation();
        break;
      case 'mqtt':
        this.mqttService?.pause();
        break;
      case 'websocket':
        this.closeWebSocket();
        break;
    }
  }

  public resume() {
    this.isPaused = false;
    this.start();
  }

  // --- MQTT Logic ---

  private connectMqtt() {
    if (this.mqttService) return;
    if (this.isPaused) return;

    this.mqttService = new MqttService(this.mqttConfig);

    this.mqttService.subscribe((packet) => {
      this.emit(packet);
    });

    this.mqttService.subscribeStatus((status: MqttConnectionStatus) => {
      this.emitStatus(status);
    });

    this.mqttService.connect();
  }

  private disconnectMqtt() {
    if (this.mqttService) {
      this.mqttService.disconnect();
      this.mqttService = null;
    }
  }

  // --- WebSocket Logic ---

  private connectWebSocket() {
    if (this.ws) return;
    if (this.isPaused) return;

    this.emitStatus('connecting');

    try {
        this.ws = new WebSocket(this.wsUrl);

        this.ws.onopen = () => {
            this.emitStatus('connected');
            if (this.reconnectTimeoutId) {
                window.clearTimeout(this.reconnectTimeoutId);
                this.reconnectTimeoutId = null;
            }
        };

        this.ws.onmessage = (event) => {
            if (this.isPaused) return;
            try {
                const rawData: RawPacketData = JSON.parse(event.data);
                PacketDecoder.decodeRawPacket(rawData).then(decodedPacket => {
                    this.emit(decodedPacket);
                });
            } catch (e) {
                console.error('Failed to parse incoming WS message:', e);
            }
        };

        this.ws.onclose = () => {
            this.emitStatus('disconnected');
            this.ws = null;

            if (this.connectionMode === 'websocket' && !this.isPaused) {
                this.reconnectTimeoutId = window.setTimeout(() => this.connectWebSocket(), 3000);
            }
        };

        this.ws.onerror = (err) => {
            console.error('WebSocket error:', err);
            this.emitStatus('error');
        };

    } catch (e) {
        console.error("Connection failed immediately", e);
        this.emitStatus('error');
        if (this.connectionMode === 'websocket' && !this.isPaused) {
            this.reconnectTimeoutId = window.setTimeout(() => this.connectWebSocket(), 3000);
        }
    }
  }

  private closeWebSocket() {
    if (this.reconnectTimeoutId) {
        window.clearTimeout(this.reconnectTimeoutId);
        this.reconnectTimeoutId = null;
    }
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.close();
      this.ws = null;
    }
    if (this.connectionMode === 'websocket') {
        this.emitStatus('disconnected');
    }
  }

  // --- Simulation Logic ---

  private startSimulation() {
    if (this.intervalId) return;
    if (this.isPaused) return;

    this.emitStatus('connected');

    this.intervalId = window.setInterval(() => {
      if (this.currentIndex >= this.packets.length) {
        this.currentIndex = 0;
      }

      const rawPacket = this.packets[this.currentIndex];
      const liveRawPacket = { ...rawPacket, ts: Date.now() / 1000 };

      PacketDecoder.decodeRawPacket(liveRawPacket).then(decodedPacket => {
        this.emit(decodedPacket);
      });
      this.currentIndex++;
    }, 1000);
  }

  private stopSimulation() {
    if (this.intervalId) {
      window.clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  private emit(packet: Packet) {
    this.callbacks.forEach((cb) => cb(packet));
  }
}
