find /home/soumitsr/.beansack/ -type f -mmin -370 | while read -r file; do
  # Calculate relative path by removing the source directory prefix
  relative_path="${file#/home/soumitsr/.beansack/}"
  # Run s5cmd cp with the relative path in the destination
  /home/soumitsr/go/bin/s5cmd --credentials-file /home/soumitsr/.aws/credentials_prod --endpoint-url $PUBLICATIONS_S3_ENDPOINT --log debug cp "$file" "s3://beansack/$relative_path"
done