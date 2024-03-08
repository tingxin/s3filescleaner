# s3filescleaner
如果一个s3 bucket桶的文件太多，直接使用控制台或者API删除会很慢，容易超时，所以用脚本分批批量清理


## 使用方式
python3 main.py --bucket amazon-connect-1a48824389c9 --prefix demo
