package updater

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"log"
)

var(
	cli *clientv3.Client
	kv clientv3.KV
	err error
	putResp *clientv3.PutResponse
	getResp *clientv3.GetResponse
	delResp *clientv3.DeleteResponse
	comResp *clientv3.CompactResponse
)

func Connect()  *clientv3.Client{
	config := clientv3.Config{}
	config.Endpoints = endpoints
	config.DialTimeout = dialTimeout

	cli, err = clientv3.New(config)
	if err != nil {
		log.Println("err:", err)
	}else {
		fmt.Printf("connnect success!\n")
	}
	return cli
}

/*
	put 操作
 */
func Put(c *clientv3.Client, key string, value string)  {
	kv := clientv3.NewKV(c)
	putResp, err = kv.Put(context.TODO(),key , value, clientv3.WithPrevKV())
	if err != nil{
		log.Fatal(err)
	}
}

/*
	get 操作
 */
func Get(c *clientv3.Client, key string) (result map[string]string) {
	kv := clientv3.NewKV(c)
	getResp, err = kv.Get(context.TODO(), key, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range getResp.Kvs {
		result[string(v.Key)] = string(v.Value)
	}
	return result
}

/*
	delete 操作
 */
func Delete(c *clientv3.Client, key string) (int64, error){
	kv = clientv3.NewKV(c)
	kv.Delete(context.TODO(), key)

	return delResp.Deleted, err
}

/*
	compact操作   只保留一个版本
 */

func Compact(c *clientv3.Client, key string) (int64, error) {
	kv := clientv3.NewKV(c)
	getResp, err = kv.Get(context.TODO(), key, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}

	revsion := getResp.Header.Revision
	comResp, err = kv.Compact(context.TODO(), revsion)

	return  comResp.Header.Revision, err
}
