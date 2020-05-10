data_dir="/archive/datasets/whois/new_data"

date_formatted=$(date -d "yesterday 13:00" '+%Y-%m-%d')
whois_password=WHOIS_PASSWORD
echo $date_formatted

wget -O ${data_dir}/newly_added/full-database-${date_formatted}.zip https://whoisds.com/your-download/direct-download-file/Developer@visably.com/${whois_password}/${date_formatted}.zip/fldb/home
cd ${data_dir}/newly_added/
unzip ${data_dir}/newly_added/full-database-${date_formatted}.zip -d $date_formatted
mv full-database-${date_formatted}.zip archive

wget -O ${data_dir}/expiring_domains/expiring-domains-${date_formatted}.zip https://whoisds.com/your-download/direct-download-file/Developer@visably.com/${whois_password}/${date_formatted}.zip/expdmn/home
cd ${data_dir}/expiring_domains/
unzip ${data_dir}/expiring_domains/expiring-domains-${date_formatted}.zip -d $date_formatted
mv expiring-domains-${date_formatted}.zip archive

wget -O ${data_dir}/country_data/country-database-${date_formatted}.zip https://whoisds.com/your-download/direct-download-file/Developer@visably.com/${whois_password}/${date_formatted}.zip/cdb/home
cd ${data_dir}/country_data/
unzip ${data_dir}/country_data/country-database-${date_formatted}.zip -d $date_formatted
mv country-database-${date_formatted}.zip archive

wget -O ${data_dir}/us_cleaned/us-cleaned-database-${date_formatted}.zip https://whoisds.com/your-download/direct-download-file/Developer@visably.com/${whois_password}/${date_formatted}.zip/uscdb/home
cd ${data_dir}/us_cleaned/
unzip ${data_dir}/us_cleaned/us-cleaned-database-${date_formatted}.zip -d $date_formatted
mv us-cleaned-database-${date_formatted}.zip archive

wget -O ${data_dir}/proxy_removed/proxy-removed-database-${date_formatted}.zip https://whoisds.com/your-download/direct-download-file/Developer@visably.com/${whois_password}/${date_formatted}.zip/prdb/home
cd ${data_dir}/proxy_removed/
unzip ${data_dir}/proxy_removed/proxy-removed-database-${date_formatted}.zip -d $date_formatted
mv proxy-removed-database-${date_formatted}.zip archive
