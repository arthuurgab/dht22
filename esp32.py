import paho.mqtt.client as mqtt
import websocket
import json
import time
import random
import math
import threading
import hmac
import hashlib

THINGSBOARD_HOST = "thingsboard.cloud"
THINGSBOARD_PORT = 1883
TB_ACCESS_TOKEN  = "esp32token123" 

SINRIC_APP_KEY    = "bda8b40c-aeb4-45b2-a060-ac664219303e" 
SINRIC_APP_SECRET = "09415d84-672c-43e0-a4ba-ab61818cdf2a-822823ee-acbc-4610-bdc0-6b700be6982d"
SINRIC_DEVICE_ID  = "6a3deae829c6be3342687174"
SINRIC_TEMP_ID    = "6a3deb3b29c6be33426871c2"  
SINRIC_WS_URL     = "wss://ws.sinric.pro"

PUBLISH_INTERVAL = 5  
TEMP_ALERT_MAX   = 35.0 
UMID_ALERT_MIN   = 20.0

device_state = {
    "ventilador": False,
    "alarme":     False,
}

_tick = 0

def tb_on_connect(client, userdata, flags, rc):
    msgs = {0:"Conectado", 1:"Protocolo inválido", 2:"ID inválido",
            3:"Servidor indisponível", 4:"Credenciais inválidas", 5:"Não autorizado"}
    print(f"[TB] {msgs.get(rc, rc)}")
    if rc == 0:
        client.subscribe("v1/devices/me/rpc/request/+")
        print("[TB] Inscrito em RPC")

def tb_on_message(client, userdata, msg):
    try:
        payload    = json.loads(msg.payload.decode())
        method     = payload.get("method", "")
        params     = payload.get("params", {})
        request_id = msg.topic.split("/")[-1]

        print(f"\n[TB-RPC] {method} | params={params}")

        if method == "setVentilador":
            device_state["ventilador"] = bool(params)
            response = {"result": device_state["ventilador"]}
        elif method == "setAlarme":
            device_state["alarme"] = bool(params)
            response = {"result": device_state["alarme"]}
        elif method == "getStatus":
            response = dict(device_state)
        else:
            response = {"error": "método desconhecido"}

        client.publish(f"v1/devices/me/rpc/response/{request_id}", json.dumps(response))
    except Exception as e:
        print(f"[TB-RPC] Erro: {e}")

def tb_on_disconnect(client, userdata, rc):
    print(f"[TB] Desconectado rc={rc}. Reconectando em 5s...")

def sinric_signature(app_secret, message):
    return hmac.new(
        app_secret.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()

def sinric_header(device_id):
    ts = int(time.time())
    return {
        "payloadVersion": 2,
        "signatureVersion": 1,
        "appKey": SINRIC_APP_KEY,
        "deviceId": device_id,
        "timestamp": ts,
    }

def sinric_send(ws, device_id, action, value):
    try:
        msg = json.dumps({
            "header": sinric_header(device_id),
            "payload": {
                "action": action,
                "deviceId": device_id,
                "value": value,
                "replyToken": f"reply-{int(time.time())}",
                "type": "event",
            }
        })
        ws.send(msg)
    except Exception as e:
        print(f"[SINRIC] Erro ao enviar evento: {e}")

_sinric_ws = None

def sinric_on_open(ws):
    global _sinric_ws
    _sinric_ws = ws
    print("[SINRIC] Conectado ao Sinric Pro")

    auth = {
        "header": {
            "payloadVersion": 2,
            "signatureVersion": 1,
        },
        "payload": {
            "action": "registerQueue",
            "appKey": SINRIC_APP_KEY,
            "deviceIds": [SINRIC_DEVICE_ID, SINRIC_TEMP_ID],
            "timestamp": int(time.time()),
        }
    }
    ws.send(json.dumps(auth))

def sinric_on_message(ws, message):
    try:
        data    = json.loads(message)
        payload = data.get("payload", {})
        action  = payload.get("action", "")
        value   = payload.get("value", {})

        print(f"\n[SINRIC] Comando: {action} | value={value}")

        if action == "setPowerState":
            state = value.get("state", "OFF") == "ON"
            device_state["ventilador"] = state
            status = "LIGADO" if state else "DESLIGADO"
            print(f"[SINRIC] Ventilador -> {status}")
            # Confirma ao Sinric Pro
            sinric_send(ws, SINRIC_DEVICE_ID, "setPowerState",
                        {"state": "ON" if state else "OFF"})

        elif action == "getCurrentTemperature":
            dados = gerar_dados()
            sinric_send(ws, SINRIC_TEMP_ID, "currentTemperature", {
                "temperature": dados["temperatura"],
                "humidity":    dados["umidade"],
            })
            print(f"[SINRIC] Temperatura enviada ao Google Home: {dados['temperatura']}°C")

    except Exception as e:
        print(f"[SINRIC] Erro on_message: {e}")

def sinric_on_error(ws, error):
    print(f"[SINRIC] Erro WebSocket: {error}")

def sinric_on_close(ws, close_status, close_msg):
    global _sinric_ws
    _sinric_ws = None
    print("[SINRIC] Desconectado. Reconectando em 10s...")

def thread_sinric():
    print("[THREAD-SINRIC] Iniciando conexão Sinric Pro...")
    while True:
        try:
            ws = websocket.WebSocketApp(
                SINRIC_WS_URL,
                header={
                    "appkey": SINRIC_APP_KEY,
                    "deviceids": f"{SINRIC_DEVICE_ID};{SINRIC_TEMP_ID}",
                    "platform": "Python",
                    "restoredevicestates": "false",
                },
                on_open    = sinric_on_open,
                on_message = sinric_on_message,
                on_error   = sinric_on_error,
                on_close   = sinric_on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[SINRIC] Exceção: {e}")
        time.sleep(10)

def thread_publicacao(tb_client):
    print("[THREAD-TB] Thread de publicação iniciada")
    while True:
        if tb_client.is_connected():
            dados   = gerar_dados()
            payload = json.dumps(dados)
            result  = tb_client.publish("v1/devices/me/telemetry", payload)

            if result.rc == 0:
                print(
                    f"[TB-PUB] Temp: {dados['temperatura']}°C | "
                    f"Umid: {dados['umidade']}% | "
                    f"HI: {dados['heat_index']}°C | "
                    f"Vent: {'ON' if dados['ventilador_status'] else 'OFF'}"
                )
                if dados["alerta_temperatura"]:
                    print(f"  ⚠️  ALERTA: Temperatura crítica ({dados['temperatura']}°C)!")
                if dados["alerta_umidade"]:
                    print(f"  ⚠️  ALERTA: Umidade baixa ({dados['umidade']}%)!")

            # Envia temperatura ao Sinric Pro também
            if _sinric_ws:
                sinric_send(_sinric_ws, SINRIC_TEMP_ID, "currentTemperature", {
                    "temperature": dados["temperatura"],
                    "humidity":    dados["umidade"],
                })
        else:
            print("[THREAD-TB] Aguardando conexão MQTT...")

        time.sleep(PUBLISH_INTERVAL)

def thread_status():
    while True:
        time.sleep(30)
        print(
            f"\n[STATUS] Ventilador: {'ON' if device_state['ventilador'] else 'OFF'} | "
            f"Alarme: {'ON' if device_state['alarme'] else 'OFF'}\n"
        )

def main():
    usar_sinric = SINRIC_APP_KEY != "SEU_APP_KEY_AQUI"

    if not usar_sinric:
        print("\n⚠️  Sinric Pro não configurado — rodando só com ThingsBoard\n")
    else:
        print("\n✅ Sinric Pro configurado — Google Home ativo\n")

    tb = mqtt.Client()
    tb.username_pw_set(TB_ACCESS_TOKEN)
    tb.on_connect    = tb_on_connect
    tb.on_message    = tb_on_message
    tb.on_disconnect = tb_on_disconnect

    print(f"[TB] Conectando em {THINGSBOARD_HOST}:{THINGSBOARD_PORT}...")
    tb.connect(THINGSBOARD_HOST, THINGSBOARD_PORT, keepalive=60)

    threads = [
        threading.Thread(target=thread_publicacao, args=(tb,), daemon=True),
        threading.Thread(target=thread_status,                  daemon=True),
    ]
    if usar_sinric:
        threads.append(threading.Thread(target=thread_sinric, daemon=True))

    for t in threads:
        t.start()

    print("[MAIN] Sistema rodando. Ctrl+C para sair.\n")
    try:
        tb.loop_forever()
    except KeyboardInterrupt:
        print("\n[MAIN] Encerrando...")
        tb.disconnect()

if __name__ == "__main__":
    main()
