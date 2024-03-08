import boto3
import argparse


REGION='ap-northeast-1'
BATCH_COUNT = 100

count_stat = 0

def loop_prefix(client, bucket, parent):
    print(f"正在查找{parent}下的文件")
    response = client.list_objects_v2(
        Bucket=bucket,
        Delimiter='/',
        Prefix=parent
    )

    if 'Contents' in response:
        objs = [obj['Key'] for obj in response['Contents'] if obj['Key'] !=parent]
    else:
        objs = []

    global count_stat
    count_stat = count_stat + len(objs)

    if  'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            t = loop_prefix(client, bucket, prefix['Prefix'])
            if t:
                objs.extend(t)
            else:
               objs.append(prefix['Prefix']) 

    print(f"已经找到{count_stat}文件...............")
    return objs

def delete_prefix(client, bucket, prefix):

    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    if 'Contents' in response:
        objects_list =[{'Key':obj['Key']} for obj in response['Contents']]
        s3.delete_objects(Bucket='bucket', Delete={'Objects': objects_list})
    print(response)
    if  'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            print(prefix)
            delete_prefix(client, bucket, prefix)
           

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket', help="s3 bucket")

    parser.add_argument(
        '--prefix', help="s3 prefix", default="")
    
    args = parser.parse_args()


    s3 = boto3.client('s3', region_name=REGION)

    prefixs = loop_prefix(client=s3, bucket=args.bucket, parent=args.prefix)
    print(len(prefixs))
    
    while len(prefixs) > 0:
        print(prefixs)
        bath = []
        for i in range(0, len(prefixs)):
            print(i)
            bath.append({'Key':prefixs[i]})
            if (i+1) % BATCH_COUNT == 0 or i ==len(prefixs) -1:
                print(f"begin delete files {bath}")
                resp = s3.delete_objects(Bucket=args.bucket, Delete={'Objects': bath})
                print(resp)
                bath = []

        prefixs = loop_prefix(client=s3, bucket=args.bucket, parent=args.prefix)
        
            



                       





           


    
