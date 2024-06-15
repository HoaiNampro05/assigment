from kafka.admin import KafkaAdminClient, NewTopic

# Cấu hình Kafka Admin client
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='nhn'
)


topic_name = "vdt2024"
num_partitions = 1
replication_factor = 1

topic_list = []
topic_list.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))

admin_client.create_topics(new_topics=topic_list, validate_only=False)
print(f"Topic vdt2024 created successfully.")

