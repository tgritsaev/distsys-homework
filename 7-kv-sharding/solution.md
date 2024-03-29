### Описание алгоритма

Я реализовал идею согласованного хэширования.

Каждый узел хранит только часть данных, каждый ключ находится ровно на одном узле.

Узлы и ключи отображаются на кольцо [0, RING_SIZE].
Узлы отображаются с помощью случайной перестановки, каждый узел представлен на кольце K раз.
Ключи отображаются с помощью хешей, ответственный за него узел - ближайший против часовой стрелки.

Всего N * K виртуальных узлов, поиск нужного узла осуществляется бинпоиском, 
поэтому итоговая сложность O(log(NK)) = log(N) + log(K) = 2 log(N)