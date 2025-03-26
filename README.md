Я створив файл docker-compose.yml, у якому визначив всі необхідні параметри для налаштування бази даних. Ось вміст файлу:
version: '3.8'
services:
  db:
    image: mysql:latest
    container_name: mysql_container
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword  
      MYSQL_DATABASE: test_db           
      MYSQL_USER: test_user              
      MYSQL_PASSWORD: test_password       
    ports:
      - "3306:3306"  # Прив'язка порту контейнера до локального хосту
    volumes:
      - db_data:/var/lib/mysql
      
volumes:
  db_data:
Цей файл дозволяє автоматично створити та запустити контейнер з базою даних MySQL.
Крок 2: Запуск Docker-контейнера
Для запуску контейнера я використав команду:
docker-compose up -d
Ця команда запускає контейнер у фоновому режимі. Після цього я перевірив його роботу за допомогою:
docker ps
Це дозволило переконатись, що контейнер працює коректно.
Крок 3: Тестове підключення до бази даних
Я перевірив підключення до бази даних через IntelliJ IDEA:
1) Перейшов до розділу Database.
2) Натиснув + → Data Source → MySQL.
3) Ввів такі дані: 1)Host: localhost 2)Port: 3306 3)User: test_user 4)Password: test_password 5)Database: test_db. 
4) Натиснув "Test Connection", щоб переконатися, що підключення встановлене
Ось команди, які я використовував у цьому проекті:
Запуск контейнера: docker-compose up -d .Перевірка запущеного контейнера:docker ps . Зупинка контейнера: docker-compose down.

