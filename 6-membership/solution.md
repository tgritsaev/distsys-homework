Детектор отказов реализован с помощью алгоритма SWIM.
Соответственно для поиска отказов посылается PING, если PING не получен, делаем PING через промежуточные вершины.

Для обновления информации существуют сообщения HEARTBEAT, когда мы передаем информацию о состоянии.

Важно, что состояние хранится в формате словаря: вершина - локальное время получения информации на вершине,
что позволяет сравнить время информации и выяснить, какая информация более актуальная.

В случае, если вершины находятся в разных группах, реализована логика получения информации с двух сторон, что позволяет
не включать обратно в множество вершину, не способную принимать сообщения, но отправляющие HEARTBEAT-ы о своем состоянии.

Для прохождения MONKEY CHAOS изменял константные параметры, для прохождения SCALABILITY увеличивал частоту посылания HEARTBEAT.