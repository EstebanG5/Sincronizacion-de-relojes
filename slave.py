# slave.py
import socket
import pickle
import random
from datetime import datetime, timedelta

# Configuración del cliente esclavo
HOST = '127.0.0.1'  # El host del maestro
PORT = 65000

# Simular un reloj local desfasado
# Cada esclavo tendrá un desfase aleatorio entre -300 y +300 segundos
TIME_OFFSET = timedelta(seconds=random.randint(-300, 300))

def get_local_time():
    """Obtiene la hora local simulada (con desfase)."""
    return datetime.now() + TIME_OFFSET

def apply_adjustment(adjustment_delta):
    """Aplica el ajuste de tiempo recibido del maestro."""
    global TIME_OFFSET
    # El algoritmo de Berkeley aplica el ajuste gradualmente,
    # pero para esta simulación, lo aplicamos de inmediato.
    TIME_OFFSET += adjustment_delta
    print(f"[{datetime.now()}] ESCLAVO: Ajuste aplicado. Nuevo desfase: {TIME_OFFSET}")

def main():
    global TIME_OFFSET
    print(f"[{datetime.now()}] ESCLAVO: Mi desfase de reloj inicial es: {TIME_OFFSET}")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, PORT))
            print(f"[{datetime.now()}] ESCLAVO: Conectado al maestro en {HOST}:{PORT}")
            
            while True:
                # 1. Esperar comando del maestro
                data = s.recv(1024)
                if not data:
                    print(f"[{datetime.now()}] ESCLAVO: Maestro desconectado.")
                    break
                
                command = data.decode('utf-8')
                
                if command == 'GET_TIME':
                    # 2. Responder con la hora local (desfasada)
                    local_time = get_local_time()
                    print(f"[{datetime.now()}] ESCLAVO: Maestro pidió la hora. Mi hora: {local_time.isoformat()}")
                    s.sendall(pickle.dumps(local_time))
                    
                    # 3. Esperar el ajuste
                    adjustment_data = s.recv(1024)
                    if not adjustment_data:
                        print(f"[{datetime.now()}] ESCLAVO: Maestro desconectado antes de enviar ajuste.")
                        break
                        
                    adjustment_delta = pickle.loads(adjustment_data)
                    print(f"[{datetime.now()}] ESCLAVO: Recibido ajuste de: {adjustment_delta}")
                    
                    # 4. Aplicar el ajuste
                    apply_adjustment(adjustment_delta)
                    print(f"[{datetime.now()}] ESCLAVO: Mi nueva hora sincronizada es: {get_local_time().isoformat()}")

        except ConnectionRefusedError:
            print(f"[{datetime.now()}] ESCLAVO: No se pudo conectar al maestro. ¿Está encendido?")
        except Exception as e:
            print(f"[{datetime.now()}] ESCLAVO: Error: {e}")
        finally:
            print(f"[{datetime.now()}] ESCLAVO: Conexión cerrada.")

if __name__ == "__main__":
    main()