# This Python code is very similar with the C# code available here: https://github.com/Azure-Samples/event-hubs-dotnet-ingest
# and it will help to complete the scenario described on this link:
    # Ingest data from event hub into Azure Data Explorer: https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-event-hub
# make sure you completed the pre-reqs on the ADX side.

from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timedelta

event_hub_name = "ehubssmadx-hub"
connection_string = "Endpoint=sb://ehub_connection_string_here="


def event_hub_ingestion():
    producer_client = EventHubProducerClient.from_connection_string(connection_string, eventhub_name=event_hub_name)

    counter = 0
    for i in range(1000):
        records_per_message = 3
        try:
            records = [
                f'{{"timeStamp": "{datetime.utcnow() + timedelta(seconds=100 * counter)}", "name": "name {counter}", "metric": {counter + recordNumber}, "source": "EventHubMessage"}}'
                for recordNumber in range(records_per_message)
            ]
            record_string = '\n'.join(records)

            event_data = EventData(record_string)
            print(f'sending message {counter}')
            # Optional "dynamic routing" properties for the database, table, and mapping you created. 
            event_data.properties = {
                "Table": "TestTable",
                "IngestionMappingReference": "TestMapping",
                "Format": "json"
            }

            event_batch = producer_client.create_batch()
            event_batch.add(event_data)

            producer_client.send_batch(event_batch)
        except Exception as exception:
            print(f'Exception: {exception}')

        counter += records_per_message

    producer_client.close()

def main():
    event_hub_ingestion()

if __name__ == '__main__':
    main()
