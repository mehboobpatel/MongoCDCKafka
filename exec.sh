printf "interakt-prod
interakt-prod-mongo-streaming
asia-south1-a
1.0.0
staging.kafka-broker-0.interakt.ai:9094,staging.kafka-broker-1.interakt.ai:9094,staging.kafka-broker-2.interakt.ai:9094
admin
DjNaoeH3pafg
mongodb+srv://kiwidev:GOsrZwc9S11373mvbnNT@interakt-development-mongo.haptikapi.com/kiwi_dev?retryWrites=true&readPreference=primary&w=1&j=true
postgresql://postgres:Fm6ZHqXRM@10.0.0.105:5432/analytics
y
" | bash -x setup.sh
