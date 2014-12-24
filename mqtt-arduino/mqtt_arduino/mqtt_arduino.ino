/*
 MQTT Arduino Publisher Sketch
 Publishes Illumination Parameter
 
 Author Shazin Sadakath
*/

#include <SPI.h>
#include <Ethernet.h>
#include <PubSubClient.h>

// Update these with values suitable for your network.
byte mac[]    = {  0xDE, 0xED, 0xBA, 0xFE, 0xFE, 0xED };
byte server[] = { 192, 168, 0, 100 };
byte ip[]     = { 192, 168, 0, 105 };

void callback(char* topic, byte* payload, unsigned int length) {
  // handle message arrived
}

EthernetClient ethClient;
PubSubClient client(server, 1883, callback, ethClient);

void setup()
{
  Serial.begin(9600);
  Ethernet.begin(mac, ip);
  if (client.connect("arduinoClient")) {
    client.publish("illumination",0);
  }
  pinMode(A0, INPUT);
}

void loop()
{
  // Read from Photoresistor
  int value = 1024 - analogRead(A0);
  char c[8];
  String str = String(value);
  // Convert to a String
  str.toCharArray(c, 8);
  // Publish to topic
  client.publish("illumination", c);
  Serial.print("sent ");
  Serial.println(c);
  client.loop();
  delay(100);
}

