package main

import (
	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"
	"io/ioutil"
	"log"
)

const (
	accessKey   = "TWO340Q8SSU2OLLX2U73"
	secret_key  = "Ca8M0JGA6bIUz4AokkB5ZHgkpd0BbPWLuFc9MLxi"
	s3_endpoint = "http://192.168.31.43:32485/"
)

var CephConn *s3.S3

func init() {

	auth := aws.Auth{
		AccessKey: accessKey,
		SecretKey: secret_key,
	}

	region := aws.Region{
		Name:                 "default",
		EC2Endpoint:          s3_endpoint, // "http://<ceph-rgw ip>:<ceph-rgw port>"
		S3Endpoint:           s3_endpoint,
		S3BucketEndpoint:     "",    // Not needed by AWS S3
		S3LocationConstraint: false, // true if this region requires a LocationConstraint declaration
		S3LowercaseBucket:    false, // true if the region requires bucket names to be lower case
		Sign:                 aws.SignV2,
	}

	CephConn = s3.New(auth, region)
}

func GetCephBucket(bucket string) *s3.Bucket {
	return CephConn.Bucket(bucket)
}

func put2Bucket(bucket *s3.Bucket, localPath, cephPath string) (*s3.Bucket, error) {
	//err := bucket.PutBucket(s3.PublicRead)
	//if err != nil {
	//	log.Fatal("-------------", err.Error())
	//	return nil, err
	//}

	bytes, err := ioutil.ReadFile(localPath)
	if err != nil {
		log.Fatal(err.Error())
		return nil, err
	}

	err = bucket.Put(cephPath, bytes, "octet-stream", s3.PublicRead)
	return bucket, err
}

func downloadFromCeph(bucket *s3.Bucket, localPath, cephPath string) error {
	data, err := bucket.Get(cephPath)
	if err != nil {
		log.Fatal(err.Error())
		return err
	}
	return ioutil.WriteFile(localPath, data, 0666)
}

func delCephData(bucket *s3.Bucket, cephPath string) error {
	err := bucket.Del(cephPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	return err
}

func delBucket(bucket *s3.Bucket) error {
	err := bucket.DelBucket()
	if err != nil {
		log.Fatal(err.Error())
	}
	return err
}

func getBatchFromCeph(bucket *s3.Bucket, prefixCephPath string) []string {
	maxBatch := 10000

	// bucket.List() 返回桶内objects的信息，默认1000条
	resultListResp, err := bucket.List(prefixCephPath, "", "", maxBatch)
	if err != nil {
		log.Fatal(err.Error())
		return nil
	}

	keyList := make([]string, 0)
	for _, key := range resultListResp.Contents {
		keyList = append(keyList, key.Key)
	}

	return keyList
}

func main() {
	bucketName := "photo"
	// 获取指定桶
	bucket := GetCephBucket(bucketName)

	// 上传
	filename := "./test.jpg"
	cephPath := "zhouzhihua/photo/test.jpg"
	bucket, err := put2Bucket(bucket, filename, cephPath)
	if err != nil {
		return
	}

	//// 下载-done
	//localPath := "./test.jpg"
	//cephPath := "zhouzhihua/photo/baby.jpg21"
	//err := downloadFromCeph(bucket, localPath, cephPath)
	//if err != nil {
	//	return
	//}

	// 获得url-done
	//cephPath := "zhouzhihua/photo/baby.jpg21"
	//url := bucket.SignedURL(cephPath, time.Now().Add(time.Hour))
	//fmt.Println(url)

	//// 批量查找-done
	//prefixCephpath := "zhouzhihua/photo"
	//lists := getBatchFromCeph(bucket, prefixCephpath)
	//fmt.Println("-------------------: ", len(lists))
	//for _, list := range lists {
	//	fmt.Println(list)
	//}

	// 删除数据
	//delCephData(bucket, cephPath)

	// 删除桶
	//delBucket(bucket)

}
