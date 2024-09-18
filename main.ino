#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h> // Incluye la biblioteca ArduinoJson
#include "time.h"
#include <Adafruit_Sensor.h>
#include <DHT.h>
#include <DHT_U.h>

#define DHTPIN 5       // Pin GPIO5 (D1) conectado al pin DATA del sensor
#define DHTTYPE DHT22  // AM2302 es equivalente a DHT22

// Definir el pin analógico donde está conectado el sensor
#define sensorPin A0
#define sensorUV A0

// Constante para convertir voltaje a irradiancia UV
const float responsividad = 0.14; // A/W (Ampere por Watt)
const float sensibilidad = 0.000000125; // 125nA (en Amperios) para 1 mW/cm²

DHT dht(DHTPIN, DHTTYPE);

//Información de la red Wifi a la que nos conectaremos y del Broker MQTT
const char* ssid = "por-aire.es_8A3E_";
const char* password = "15373663";

//Direccion IP del broker al que nos vamos a conectar
const char* IP_mqtt_server = "192.169.0.102";

//Variables para conseguir la hora de un server de internet
const char* ntpServer = "pool.ntp.org"; // Servidor NTP
const long  gmtOffset_sec = 3600;          // Ajuste de la zona horaria en segundos (GMT)
const int   daylightOffset_sec = 3600;  // Ajuste para horario de verano (si aplica)

//Creamos un cliente Wifi para conectarnos a la Wifi
WiFiClient espClient;
PubSubClient client(espClient);

//Topic para el broker MQTT
const char* topic1 = "ABV/huerto/tomate1";

// Usuario y contraseña para la conexión al broker MQTT
const char* mqtt_user = "user_abv";
const char* mqtt_password = "Tios1991m";

char fecha[25];  // Arreglo de caracteres para almacenar la fecha

void setup() {
  delay(1000);

  // Inicia la comunicación serial a 9600 baudios
  Serial.begin(9600);

  dht.begin();          // Iniciar el sensor
  
  // Semilla para generar números aleatorios basada en el ruido eléctrico del pin A0
  randomSeed(analogRead(0));

  //Inicion conexion WiFi
  WiFi.begin(ssid, password);

  // Espera a que se establezca la conexión WiFi
  Serial.print("Conectando a WiFi");
  delay(500);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nConexión WiFi establecida.");
  Serial.print("Dirección IP: ");
  Serial.println(WiFi.localIP());

  // Configura el servidor MQTT
  client.setServer(IP_mqtt_server, 1883);

}


void loop() {
  

  // Comprueba si el cliente MQTT está conectado, si no, intenta reconectarse
  if (!client.connected()) {
    if(client.connect("ESP8266Client", mqtt_user, mqtt_password)) {
      Serial.println("Conectado al broker MQTT.");
    } else {
      Serial.print("Fallo en la conexión al broker MQTT. Estado: ");
      Serial.println(client.state());
      delay(5000); // Espera antes de intentar reconectar
      return;
    }
  }
  
  // Leer la humedad y la temperatura
  float humedad_ambiente = dht.readHumidity();
  float temperatura = dht.readTemperature();

  // Comprobar si la lectura es válida
  if (isnan(humedad_ambiente) || isnan(temperatura)) {
    Serial.println("Error al leer del sensor DHT!");
    return;
  }

  // Leer el valor analógico del sensor
  int valorSensor = analogRead(sensorPin);

  // Convertir el valor del sensor a porcentaje (opcional)
  float humedad_suelo = map(valorSensor, 0, 1023, 100, 0);

  // Leer el valor analógico del sensor (0-1023)
  int valorUV = analogRead(sensorUV);

  // Convertir el valor leído en voltaje (0-3.3V)
  float voltaje = (valorUV / 1023.0) * 3.3;

  // Calcular la corriente de salida del sensor
  // El sensor tiene una sensibilidad de 125nA por 1mW/cm², lo que significa que genera
  // 125nA de corriente por cada 1mW/cm² de irradiancia UV.
  float corriente = voltaje / 1000000; // Convertimos voltaje a corriente (asumiendo alta impedancia)

  // Calcular la irradiancia UV en mW/cm² usando la responsividad
  float irrad = corriente / (responsividad * sensibilidad); 

  // Inicializar la obtención del tiempo desde el servidor NTP
  configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);

  // Esperar a que se sincronice el tiempo
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) {
    Serial.println("Fallo al obtener la hora");
    return;
  }

  // Formatear la fecha y hora en formato YYYY-MM-DD HH:MM:SS
  strftime(fecha, sizeof(fecha), "%Y-%m-%d %H:%M", &timeinfo);

  // Imprimir la fecha y hora formateada
  Serial.println("Fecha y hora actual: " + String(fecha));
  
  // Crear un objeto JSON
  StaticJsonDocument<200> doc;
  doc["Zona"] = "Tomate1";
  doc["Hora"] = fecha;
  doc["T_amb"] = temperatura;
  doc["HR_amb"] = humedad_ambiente;
  doc["HR_soil"] = humedad_suelo;
  doc["irrad"] = irrad;

  // Serializa el objeto JSON a una cadena
  char jsonBuffer[512];
  serializeJson(doc, jsonBuffer);

  // Imprimir el JSON en el monitor serie
  Serial.println(jsonBuffer);

  // Publicar el JSON en el broker MQTT
  client.publish(topic1, jsonBuffer);

  // Marcar otro bucle de envío MQTT
  client.loop();

  // Esperar un poco entre lecturas
  delay(60000);
  
}

