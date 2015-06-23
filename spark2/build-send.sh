rm -rf target
mvn package
scp -i ~/Documents/recsysdata/hadoop.pem ./target/altc-1.0-SNAPSHOT.jar ubuntu@10.214.208.111:/home/ubuntu/givelab
