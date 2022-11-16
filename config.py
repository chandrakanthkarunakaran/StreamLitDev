# environments: [development] [production]; suffix: [-DEV,-PROD]

env='development'

#env='production'

if env=='development':
    regionName='us-east-1'
    suffixS3='-dev'
    suffixDb='-DEV'
    flaskAPI='http://44.206.150.114/flask/root'
    ec2IP='44.206.150.114'

# elif env=='production':
#     regionName='us-west-1'
#     suffixS3='-prod'
#     suffixDb=''
#     flaskAPI='http://54.219.248.96/flask/root'
#     ec2IP="54.219.248.96"

elif env=='production':
    regionName='ap-south-1'
    suffixS3='-prod'
    suffixDb=''
    flaskAPI='http://3.110.93.210/flask/root'
    ec2IP="3.110.93.210"

