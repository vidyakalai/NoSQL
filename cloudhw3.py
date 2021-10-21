import boto3
import os
import csv

#created profile to pass credentials
os.environ['AWS_PROFILE'] = "cloudhw"
s3 = boto3.resource('s3')

#COMMENTED OUT AFTER CREATION
#try:
#    s3.create_bucket(Bucket='databucket6464lala')
#except Exception as e:
#    print (e)
#bucket = s3.Bucket("databucket6464lala")
#bucket.Acl().put(ACL='public-read')

#upload a new object into the bucket
#body = open('C:\Users\Vidya\Downloads\hw3data\datafiles\exp3.csv', 'rb')
#o = s3.Object('databucket6464lala', 'exp3.csv').put(Body=body )
#s3.Object('databucket6464lala', 'exp3.csv').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',region_name='us-east-1')
# try:
#     table = dyndb.create_table(
#         TableName='DataTable',
#         KeySchema=[
#             {
#                 'AttributeName': 'PartitionKey',
#                 'KeyType': 'HASH'
#             },
#             {
#                 'AttributeName': 'RowKey',
#                 'KeyType': 'RANGE'
#             }
#         ],
#         AttributeDefinitions=[
#             {
#                 'AttributeName': 'PartitionKey',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'RowKey',
#                 'AttributeType': 'S'
#             },
#         ],
#         ProvisionedThroughput={
#             'ReadCapacityUnits': 5,
#             'WriteCapacityUnits': 5
#         }
#     )
# except Exception as e:
#     print (e)
#if there is an exception, the table may already exist. if so...
table = dyndb.Table("DataTable")
#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
#print(table.item_count)

# with open('C:\Users\Vidya\Downloads\hw3data\experiments.csv', 'rb') as csvfile:
#     csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
#     for item in csvf:
#         print(item)
#         body = open('C:\Users\Vidya\Downloads\hw3data\datafiles\\'+item[3], 'rb')
#         s3.Object('databucket6464lala', item[3]).put(Body=body )
#         md = s3.Object('databucket6464lala', item[3]).Acl().put(ACL='public-read')
#         url = " https://s3-us-east-1.amazonaws.com/databucket6464lala/"+item[3]
#         metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description' : item[4], 'conductivity' : item[2], 'url':url}
#         try:
#             table.put_item(Item=metadata_item)
#         except:
#             print("item may already be there or another failure")

response = table.get_item(
Key={
    'PartitionKey': 'Experiment 1',
    'RowKey': '1'
}
)
item = response['Item']
print(item)