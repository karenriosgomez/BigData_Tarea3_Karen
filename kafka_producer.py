import csv
import json
import time
from kafka import KafkaProducer

# Configurar el productor conectado al broker local de la VM
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'accidentes_colombia'
archivo_csv = 'lesiones_accidentes_transito.csv'

print(f"=== Iniciando Simulador de Streaming de Accidentes ===")
print(f"Enviando datos al topic: '{topic_name}'... Presiona Ctrl+C para detener.")

try:
    with open(archivo_csv, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Enviar la línea del CSV convertida en JSON a Kafka
            producer.send(topic_name, value=row)
            print(f"Evento enviado: {row.get('municipio', 'Desconocido')} - {row.get('departamento', 'Desconocido')}")
            
            # Simular un retraso de 1 segundo entre accidentes para el tiempo real
            time.sleep(1.0)
except FileNotFoundError:
    print(f"Error: No se encontró el archivo '{archivo_csv}' en este directorio.")
except KeyboardInterrupt:
    print("\nSimulación detenida por el usuario.")
finally:
    producer.close()
    print("=== Productor de Kafka Cerrado ===")
