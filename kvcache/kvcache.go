package kvcache

import (
	"context"
	"errors"
	"log"
	"time"
)

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

var (
	ErrNotExists      = errors.New("not exists")
	ErrRequestTimeout = errors.New("request timeout")
	ErrClosed         = errors.New("cache closed")
)

// TODO: подумать над значениями констант
const (
	putBufSise     = 10                      // когда клиент (методы Get,MGet,...) сам сходил в db он обязан поделиться свежими
	mputBufSize    = 10                      // данными с кэшем через канал put(mput,...). Чтобы клиент не ждал, эти каналы буферизируются.
	requestTimeout = 200 * time.Millisecond  // метод Get не ждет ответа кэша больше этого времени
	cacheTTL       = 1000 * time.Millisecond // время актуальности кэша (устное вводное из ролика)
	keyTTL         = 4 * time.Hour           // при обновлении кэша запись будет удалена, если в течении этого время никто ее не запрашивал
	presetСacheCap = 1024
)

type getResponse struct {
	key    string // WTF: мы где нибудь используем это поле? Если нет, зачем здесь эти лишние 16 байт?
	val    string // WTF: IPv4=4byte, почему строка? - нам приходит/уходит строка. Это одна и тажа строка, а при конвертации мы будем плодить новые строки.
	err    error
	expire time.Time // WTF: Time это (по крайней мере) 24 байт. Может хватит int64?
}

type getRequest struct {
	key string
	out chan<- *getResponse
}

type mgetResponse struct {
	keys []string
	vals []*string
}

type mgetRequest struct {
	keys []string
	out  chan<- []*string
}

type Cache struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	db      KVDatabase
	get     chan *getRequest
	put     chan *getResponse
	mget    chan *mgetRequest
	mput    chan *mgetResponse
	cache   map[string]*getResponse
	roCache map[string]*getResponse // сюда премещается кэш на время обновления
}

func Open(ctx context.Context, db KVDatabase) *Cache {
	ctx, cancel := context.WithCancelCause(ctx)
	c := &Cache{
		ctx:    ctx,
		cancel: cancel,
		db:     db,
		get:    make(chan *getRequest),
		put:    make(chan *getResponse, putBufSise), // буфер, чтобы не задерживать клиента
		mget:   make(chan *mgetRequest),
		mput:   make(chan *mgetResponse, mputBufSize),
		cache:  make(map[string]*getResponse, presetСacheCap),
	}
	go c.serve()
	return c
}

func (c *Cache) Close() {
	c.cancel(ErrClosed)

	// TODO: отследить завершение serve
}

func (c *Cache) Get(key string) (string, error) {
	if err := context.Cause(c.ctx); err != nil {
		return "", err
	}

	ch := make(chan *getResponse, 1) // буфер на случай таймаута
	c.get <- &getRequest{
		key: key,
		out: ch,
	}

	select {
	case <-time.After(requestTimeout):
		return "", ErrRequestTimeout
	case resp := <-ch:
		if resp != nil {
			return resp.val, resp.err
		}
	}

	val, err := c.db.Get(key) // 100ms
	c.put <- &getResponse{
		key: key,
		val: val,
		err: err,
	}

	return val, err
}

func (c *Cache) Keys() ([]string, error) {
	if err := context.Cause(c.ctx); err != nil {
		return nil, err
	}

	// TODO: можно организовать кэширование ответа на время cacheTTL

	return c.db.Keys() // 100ms
}

func (c *Cache) MGet(keys []string) ([]*string, error) {
	if err := context.Cause(c.ctx); err != nil {
		return nil, err
	}

	// TODO: сейчас, если в кэше нет полного ответа, весь запрос
	//  отправляется к удаленному серверу.
	//  Можно организовать получение частичного ответа от кэша, чтобы
	//  не запрашивать по второму разу то, что уже попало в кэш.
	//  А можно (учитывая, что один запрос в минуту) не опрашивать кэш,
	//  а сразу идти в db на удаленый сервер.

	ch := make(chan []*string)
	c.mget <- &mgetRequest{
		keys: keys,
		out:  ch,
	}
	if vals := <-ch; vals != nil {
		return vals, nil
	}

	vals, err := c.db.MGet(keys) // 100ms
	if err != nil {
		return vals, err
	}

	c.mput <- &mgetResponse{keys, vals}

	return vals, err
}

func (c *Cache) serve() {
	const op = "Cache.serve"

	// подписки на единичные запросы, чтобы не ходить одновременно за одним и тем жe
	ss := subscribers{}
	defer ss.closeAll(context.Cause(c.ctx))

	// канал для запроса обновления кэша
	getUpdate := make(chan struct{})
	defer close(getUpdate)

	// канал для получения обновления кэша (буферизируем, на случай закрытия во время обновления)
	update := make(chan map[string]*getResponse, 1)

	go func() {
		for range getUpdate {
			cache, err := c.getCacheUpdate(c.roCache)
			if err != nil {
				log.Printf("%s: can't get cache update: %v", op, err)
				update <- nil // oops!..
				continue
			}
			update <- cache
		}
	}()

	// временный rw-кэш на время обновления (во время обновления основной кэш пререводися в ro)
	tmpCache := map[string]*getResponse{}
	var isUpdating bool

	// переодичность обновления кэша
	tk := time.NewTicker(cacheTTL)
	defer tk.Stop()

loop:
	for c.ctx.Err() == nil {
		select {
		case <-c.ctx.Done():
			break loop

		case <-tk.C:
			if isUpdating {
				log.Printf("%s: cache update was skipped because it is already in progress", op)
				continue loop
			}
			isUpdating = true
			c.roCache = c.cache
			c.cache = tmpCache
			getUpdate <- struct{}{}

		case cache := <-update:
			if cache == nil { // приходит nil, если удаленная сторона вернула ошибку
				c.cache = c.roCache
			} else {
				c.cache = cache
			}
			c.roCache = nil

			for k, v := range tmpCache {
				c.cache[k] = v
			}
			clear(tmpCache)

			isUpdating = false

		case req := <-c.get:
			if resp := c.getFromCache(req.key); resp != nil {
				resp.expire = time.Now().Add(keyTTL)
				req.out <- resp
				continue loop
			}

			// существует ли подписка на запрос?
			if s, ok := ss[req.key]; !ok {
				// создаем новую подписку и просим, чтобы сам сходил за ответом
				ss[req.key] = nil
				req.out <- nil
			} else {
				// уже кого-то пошел за ответом - подписываем на получение ответа
				ss[req.key] = append(s, req.out)
			}

		case resp := <-c.put:
			// принесли ответ - обновляем кэш и раздаем ответ подписчикам
			resp.expire = time.Now().Add(keyTTL)
			c.cache[resp.key] = resp
			ss.send(resp)

		case req := <-c.mget:
			expire := time.Now().Add(keyTTL)

			vals := make([]*string, len(req.keys))
			for i, key := range req.keys {
				rec := c.getFromCache(key)

				if rec == nil {
					// в кэше нет полного ответа - сходи сам
					req.out <- nil
					continue loop
				}

				rec.expire = expire

				if rec.err == nil {
					vals[i] = &rec.val
				}
			}
			req.out <- vals

		case resp := <-c.mput:
			expire := time.Now().Add(keyTTL)

			for i, key := range resp.keys {
				if pv := resp.vals[i]; pv == nil {
					c.cache[key] = &getResponse{
						key:    key,
						err:    ErrNotExists,
						expire: expire,
					}
				} else {
					c.cache[key] = &getResponse{
						key:    key,
						val:    *pv,
						expire: expire,
					}
				}
			}
		}
	}
}

func (c *Cache) getFromCache(key string) *getResponse {
	if c.roCache != nil {
		if resp := c.roCache[key]; resp != nil {
			return resp
		}
	}
	if resp := c.cache[key]; resp != nil {
		return resp
	}
	return nil
}

func (c *Cache) getCacheUpdate(cache map[string]*getResponse) (map[string]*getResponse, error) {

	// TODO: память keys, expires и старого кэша (но осторожно) можно не выбрасывать,
	//  а использовать повторно при следующем обновлении

	now := time.Now()
	keys := make([]string, 0, len(cache))

	// expires это единственно поле (кроме ключа) которое сохраняется для записи из
	// старого кэша. Мы создаем под него слайс, т.к. получить его из слайса гораздо
	// быстрее, чем идти заново в мапу по ключу
	expires := make([]time.Time, 0, len(cache))

	for key, rec := range cache {
		// будем обновлять только "не протухшие" записи
		if rec.expire.After(now) {
			keys = append(keys, key)
			expires = append(expires, rec.expire)
		}
	}

	vals, err := c.db.MGet(keys) // 100ms
	if err != nil {
		return nil, err
	}

	cache = make(map[string]*getResponse, len(keys)*2) // TODO: подумать над capacity
	data := make([]getResponse, len(keys))
	for i, key := range keys {
		pv := vals[i]
		rec := &data[i]
		if pv == nil {
			*rec = getResponse{
				key:    key,
				err:    ErrNotExists,
				expire: expires[i],
			}
		} else {
			*rec = getResponse{
				key:    key,
				val:    *pv,
				expire: expires[i],
			}
		}
		cache[key] = rec
	}

	return cache, nil
}

type subscribers map[string][](chan<- *getResponse)

func (ss subscribers) closeAll(err error) {
	resp := &getResponse{err: err}
	for _, s := range ss {
		for _, out := range s {
			out <- resp
		}
	}
	clear(ss)
}

func (ss subscribers) send(resp *getResponse) {
	if s, ok := ss[resp.key]; ok {
		for _, out := range s {
			out <- resp
		}
		delete(ss, resp.key)
	}
}
