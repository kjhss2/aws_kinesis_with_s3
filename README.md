# AWS Kinesis 파이프라인 가이드

## 파이프라인 요약

    AWS Kinesis -> AWS Firehose -> AWS S3 -> DB Insert

1.  AWS 준비

    1-1. AWS Kinesis Stream 생성

    - 리전 고려

    1-2. AWS Firehose 생성

    - target / source 지정
    - 리전 고려
    - S3 지정
    - 저장하려는 namespace/logs/yyyy/mm/dd/hh 형식으로 저장 가능
    - 동적 파니셔닝 필요시 추가
      - tableName 등으로 별도 저장 가능

2.  AWS Kinesis record put

    2-1. Client 대상으로부터 record data 들을 AWS Kinesis Stream 으로 put

    - @aws-sdk/client-kinesis PutRecordsCommand를 이용

3.  S3에 쌓인 file을 다운로드 하여 DB에 저장

    3-1. 사용 npm lib

        @aws-sdk/client-s3S3 s3 file load

        ListObjectsV2Command

        GetObjectCommand

    3.2 1회 조회 시 s3 file 목록을 1000개 씩 가져 와서 처리

    3.2 배치처리를 이용하여 한 번에 1000건 씩 DB Insert
