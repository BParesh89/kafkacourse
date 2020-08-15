## Problem Statement
Create a Kafka topic that will receive a key-value pair of stock ticks.
Send Following messages to the topic.

HDFCBANK:1250.00

TCS:2150.00

KOTAK:1570

HDFCBANK:1255.00

HDFCBANK:

HDFCBANK:1260

Create Kafka Streams Application for the following:
1. Read the topic as KTable.
2. Filter out all other symbols except HDFCBANK and TCS.
3. Store the filtered messages in a separate KTable.
4. Show the contents of the Final KTable.