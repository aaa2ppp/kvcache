Задача с открытого собеседования

Собеседование Senior Go-разработчика / Даниил Подольский, Владимир Балун - Антон Зиновьев
https://www.youtube.com/watch?v=GD0iHLucYdU


``` go
// У нас есть key-value база данных, в которой хранятся пользовательские IPv4 адреса.
// Эта база данных находится достаточно далеко от пользователей, из-за чего мы получаем
// дополнительную latency в сотню миллисекунд. Мы хотим минимизировать это время и начали
// думать в сторону кэширования...

// Нужно написать кэш для key-value базы данных, при этом важно учесть:
// - чтобы получился максимально эффективный и понятный код без багов
// - чтобы, пользовательский код ничего не знал про кэширование


type KVDatabase interface {
	Get(key string) (string, error)        // get single value bya key (users use it very often)
	Keys() ([]string, error)               // get all keys (users use it very seldom)
	MGet(keys []string) ([]*string, error) // get values by keys (users use it very seldom)
}

// Get это 500rps...
// Keys, MGet - может один раз в минуту...
// Давай сделаем инвалидацию всей мапы...

```