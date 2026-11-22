<?php
// SPDX-License-Identifier: GPL-3.0-only
require $argv[1];
$s3 = new Aws\S3\S3Client([
 'version'=>'latest','region'=>'eu-west-1','endpoint'=>$argv[2],
 'credentials'=>['key'=>$argv[3],'secret'=>$argv[4]],'use_path_style_endpoint'=>true,
]);
function check($condition) { if (!$condition) throw new Exception('S3 SDK assertion failed'); }
$bucket='fop-files';
$body=str_repeat("FOP streaming payload\n",1000000);
$f=fopen('php://temp/maxmemory:1048576','w+');fwrite($f,$body);rewind($f);
$put=$s3->putObject(['Bucket'=>$bucket,'Key'=>'123:','Body'=>$f,'ACL'=>'private','ContentType'=>'text/plain','Metadata'=>['owner'=>'fop']]);fclose($f);
check($put['ETag']==='"'.md5($body).'"');
$get=$s3->getObject(['Bucket'=>$bucket,'Key'=>'123:']);check((string)$get['Body']===$body);check($get['Metadata']['owner']==='fop');
$head=$s3->headObject(['Bucket'=>$bucket,'Key'=>'123:']);check($head['ContentLength']===strlen($body));
$range=$s3->getObject(['Bucket'=>$bucket,'Key'=>'123:','Range'=>'bytes=2-8']);check((string)$range['Body']===substr($body,2,7));
$tmp=tempnam(sys_get_temp_dir(),'stqr-s3-php-');try{$s3->getObject(['Bucket'=>$bucket,'Key'=>'123:','SaveAs'=>$tmp]);check(file_get_contents($tmp)===$body);}finally{unlink($tmp);}
foreach(['123:100_100','folder/a','folder/b'] as $key)$s3->putObject(['Bucket'=>$bucket,'Key'=>$key,'Body'=>$key,'ACL'=>'private']);
$list=$s3->listObjects(['Bucket'=>$bucket,'Prefix'=>'123:','MaxKeys'=>100]);check(count($list['Contents'])===2);
$count=0;foreach($s3->getPaginator('ListObjectsV2',['Bucket'=>$bucket,'MaxKeys'=>1]) as $page)$count+=count($page['Contents']);check($count===4);
$presigned=$s3->createPresignedRequest($s3->getCommand('GetObject',['Bucket'=>$bucket,'Key'=>'123:100_100']),'+5 minutes');
check(file_get_contents((string)$presigned->getUri())==='123:100_100');
foreach(['123:','123:100_100','folder/a','folder/b'] as $key)$s3->deleteObject(['Bucket'=>$bucket,'Key'=>$key]);
check(count($s3->listObjects(['Bucket'=>$bucket])['Contents']??[])===0);
echo "STQR_S3_PHP_PASS\n";
