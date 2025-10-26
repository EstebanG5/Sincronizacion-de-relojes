# master.py
import socket
import threading
import pickle
from datetime import datetime, timedelta

# Configuración del servidor maestro
HOST = '127.0.0.1'  # localhost
PORT = 65000
NUM_SLAVES = 2
clients = []
lock = threading.Lock()

def get_time_as_timestamp(dt):
    """Convierte un objeto datetime a timestamp (float)"""
    return dt.timestamp()

def get_time_from_timestamp(ts):
    """Convierte un timestamp (float) a objeto datetime"""
    return datetime.fromtimestamp(ts)

def start_sync_loop():
    """Inicia el bucle de sincronización del Algoritmo de Berkeley."""
    print(f"[{datetime.now()}] COORDINADOR: Iniciando bucle de sincronización...")
    
    while True:
        # Espera un ciclo
        threading.Event().wait(15) # Sincroniza cada 15 segundos
        
        if len(clients) < NUM_SLAVES:
            print(f"[{datetime.now()}] COORDINADOR: Esperando más esclavos... ({len(clients)}/{NUM_SLAVES})")
            continue

        print(f"\n[{datetime.now()}] COORDINADOR: --- Iniciando nuevo ciclo de sincronización ---")
        times = {}
        
        # 1. Incluir la hora del maestro
        master_time = datetime.now()
        times['master'] = master_time
        print(f"[{datetime.now()}] COORDINADOR: Mi hora local es: {master_time.isoformat()}")

        # 2. Pedir la hora a todos los esclavos
        with lock:
            # Usamos una copia para evitar problemas de concurrencia si un cliente se desconecta
            current_clients = list(clients)
            
            for conn, addr in current_clients:
                try:
                    # Enviar comando para pedir la hora
                    conn.sendall(b'GET_TIME')
                    
                    # Recibir la hora del esclavo
                    data = conn.recv(1024)
                    if not data:
                        raise Exception("Cliente desconectado")
                        
                    slave_time = pickle.loads(data)
                    times[addr] = slave_time
                    print(f"[{datetime.now()}] COORDINADOR: Recibida hora de {addr}: {slave_time.isoformat()}")
                    
                except Exception as e:
                    print(f"[{datetime.now()}] COORDINADOR: Error con {addr}: {e}. Eliminando cliente.")
                    clients.remove((conn, addr))
                    conn.close()

        # 3. Calcular el promedio
        if len(times) <= 1:
            print(f"[{datetime.now()}] COORDINADOR: No hay suficientes relojes para sincronizar. Saltando ciclo.")
            continue
            
        timestamps = [get_time_as_timestamp(t) for t in times.values()]
        avg_timestamp = sum(timestamps) / len(timestamps)
        avg_datetime = get_time_from_timestamp(avg_timestamp)
        
        print(f"[{datetime.now()}] COORDINADOR: Hora promedio calculada: {avg_datetime.isoformat()}")

        # 4. Calcular ajustes (deltas)
        adjustments = {}
        for key, time_val in times.items():
            # (Hora Promedio) - (Hora Local) = Ajuste
            adjustment_delta = avg_datetime - time_val
            adjustments[key] = adjustment_delta
            
        # 5. Enviar ajustes a los esclavos
        with lock:
            # Usamos la lista original 'clients'
            for conn, addr in clients:
                if addr in adjustments:
                    try:
                        adjustment_data = pickle.dumps(adjustments[addr])
                        conn.sendall(adjustment_data)
                        print(f"[{datetime.now()}] COORDINADOR: Enviando ajuste de {adjustments[addr]} a {addr}")
                    except Exception as e:
                        print(f"[{datetime.now()}] COORDINADOR: No se pudo enviar ajuste a {addr}: {e}")
                        
        # 6. Aplicar ajuste al maestro (en esta simulación solo lo mostramos)
        master_adjustment = adjustments.get('master')
        if master_adjustment:
            print(f"[{datetime.now()}] COORDINADOR: Mi ajuste local es: {master_adjustment}")
            # En un sistema real: set_local_time(master_time + master_adjustment)


def handle_client(conn, addr):
    """Maneja la conexión de un nuevo cliente esclavo."""
    print(f"[{datetime.now()}] COORDINADOR: Nuevo esclavo conectado: {addr}")
    with lock:
        clients.append((conn, addr))
    
    # El hilo se mantiene vivo para mantener la conexión,
    # pero la lógica principal está en el 'start_sync_loop'
    try:
        while True:
            # Esperamos a que la conexión se cierre por parte del cliente
            # recv(1) con flag MSG_PEEK no consume el buffer
            data = conn.recv(1, socket.MSG_PEEK)
            if not data:
                break
            threading.Event().wait(1.0) # Pequeña pausa
    except (ConnectionResetError, BrokenPipeError):
        print(f"[{datetime.now()}] COORDINADOR: Esclavo {addr} desconectado abruptamente.")
    finally:
        print(f"[{datetime.now()}] COORDINADOR: Esclavo {addr} desconectado.")
        with lock:
            # Remover el cliente si todavía existe en la lista
            client_tuple = (conn, addr)
            if client_tuple in clients:
                clients.remove(client_tuple)
        conn.close()


def main():
    # Iniciar el hilo de sincronización
    sync_thread = threading.Thread(target=start_sync_loop, daemon=True)
    sync_thread.start()

    # Configurar el socket del servidor
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"[{datetime.now()}] COORDINADOR: Servidor maestro escuchando en {HOST}:{PORT}")
        print(f"Esperando {NUM_SLAVES} esclavos...")

        while True:
            conn, addr = s.accept()
            # Iniciar un hilo para manejar al cliente
            client_thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            client_thread.start()

if __name__ == "__main":
    main()