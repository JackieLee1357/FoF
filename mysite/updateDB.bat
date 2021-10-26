echo 创建迁移文件
python manage.py makemigrations
echo 执行迁移并使用您的模型更改更新数据库
python manage.py migrate
pause
