# Домашние задания РС

Чтобы сдать задание, нужно:

1. [только в первый раз] Сделать **приватный** fork, выдать доступы преподавателям и ассистентам: @sol, @NanoBjorn, @murfel, @alexmir, @petuhovskiy, @SpeedOfMagic, @RedClusive, @alex.shitov1237, @KiK0S, @lodthe, @AlPerevyshin.
2. [только в первый раз] Скачать его к себе с сабмодулями, то есть `git clone --recurse-submodules YOUR_REPOSITORY`
3. [только в первый раз] Добавить оригинальный репозиторий как второй remote, чтобы скачивать из него обновления: `git remote add upstream git@gitlab.com:NanoBjorn/distsys-homework.git`
4. Синхронизироваться с апстримом: `git fetch upstream && git checkout main && git merge upstream/main && git push`
5. Создать ветку для своего задания `git checkout -b N-taskname`
6. Написать код, протестировать локально по инструкции в задании
7. Запушить ветку `git push -u origin HEAD`
8. Открыть Merge Request в свою ветку `main`
9. До дедлайна сдать ссылку на Merge Request в соответствующее задание в Canvas
10. По завершению ревью нажать Merge
