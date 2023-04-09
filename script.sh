#!/bin/bash

# vim /etc/clickhouse-server/config.xml
# set <max_suspicious_broken_parts>100000000</max_suspicious_broken_parts>

rm -rf export
rm -f export.tgz

gcloud compute ssh --zone us-central1-a test-plumb --command="bash -s" <<'EOF'
    export user_path=$(pwd) 

    sudo -E su
    rm -rf $user_path/export
    rm -f $user_path/export.tgz
    mkdir $user_path/export

    cd /var/lib/clickhouse/metadata

    for db in */; do
        db=${db:0:$((${#db} - 1))}
        if [[ $db == "terra" ]]; then
            mkdir $user_path/export/$db
            cd $db

            for table in *; do
                table=${table:0:$((${#table} - 4))}

                cp $table.sql $user_path/export/$db/$table.sql
                type=$(head -n 1 $user_path/export/$db/$table.sql | awk '{print $2}')
                first_char_type=${type:0:1}
                substitute="CREATE $type IF NOT EXISTS $table"
                sed -i "1s/.*/$substitute/" $user_path/export/$db/$table.sql
                cp $user_path/export/$db/$table.sql $user_path/export/$db/$table.$first_char_type.sql

                if [[ $type == "TABLE" ]]; then
                    query="SELECT * FROM $db.$table"         
                    clickhouse client -q "${query}"  --format CSV >> $user_path/export/$db/$table.csv   
                fi  
            done

            cd ..
        fi
    done

    cd $user_path
    tar cfz export.tgz export
EOF

gcloud compute scp --zone us-central1-a test-plumb:export.tgz export.tgz
tar xvzf export.tgz

cd export

for database in */; do
    database=${database:0:$((${#database} - 1))}
    clickhouse-client -q "CREATE DATABASE IF NOT EXISTS ${database}"

    cd $database
    tables=$(ls *.T.sql 2> /dev/null || echo "")
    for table in $tables; do
        query=$(cat $table)
        clickhouse-client --database=$database -q "${query}"
    done

    tables=$(ls *.V.sql 2> /dev/null || echo "")
    for table in $tables; do
        query=$(cat $table)
        clickhouse-client --database=$database -q "${query}"
    done

    for table in *.csv; do
        table=${table:0:$((${#table} - 4))}
        if [ -s $table.csv ]; then
            cat $table.csv | clickhouse-client --database=$database --query="INSERT INTO ${table} FORMAT CSV";
        fi
    done
    cd .. 
done
