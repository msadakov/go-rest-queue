package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

var port = flag.String("p", "8080", "webserver port number")

func main() {
	flag.Parse()

	store := newStore()
	pool := NewHandlerPool()

	cfg := &queueHandlerConfig{
		store: store,
		pool:  pool,
	}

	http.HandleFunc("/", queueHandler(cfg))

	httpServer := http.Server{
		Addr: ":" + *port,
	}

	// Канал для ожидания закрытия соединений.
	idleConnectionsClosed := make(chan struct{})

	// Ожидание сигнала SIGINT для graceful выключения.
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
		// Ожидаем, когда все запросы завершатся.
		if err := httpServer.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
		close(idleConnectionsClosed)
	}()

	// Запускаем HTTP сервер.
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}

	<-idleConnectionsClosed
}

// queueHandlerConfig - это необходимая конфигурация для обработчиков запросов.
type queueHandlerConfig struct {
	store *store
	pool  *channelPool
}

// queueHandler обеспечивает разделение обработки по методам запроса.
func queueHandler(cfg *queueHandlerConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			cfg.putQueueHandler(w, r)
		case http.MethodGet:
			cfg.getQueueHandler(w, r)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}

// putQueueHandler обработчик PUT запроса, который кладёт данные в очередь.
func (c *queueHandlerConfig) putQueueHandler(w http.ResponseWriter, r *http.Request) {
	name, ok := getQueueName(r.URL)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	msg, ok := getQueueMessage(r.URL)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Попытка отправить сообщение обработчикам.
	isSended := c.pool.sendMessage(name, msg)
	// Если нет обработчиков ожидающих сообщения, то кладём в хранилище.
	if !isSended {
		c.store.push(name, msg)
	}
}

// getQueueHandler обработчик GET запроса на получение данных из очереди.
func (c *queueHandlerConfig) getQueueHandler(w http.ResponseWriter, r *http.Request) {
	name, ok := getQueueName(r.URL)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	timeout, hasTimeout := getRequestTimeout(r.URL)

	// Пробуем получить сообщение из хранилища.
	msg, ok := c.store.pop(name)
	// Если нет сообщений и не нужно ждать сообщения, то выводим 404.
	if !ok && !hasTimeout {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Вывод сообщения если оно есть.
	if ok {
		w.Write([]byte(msg))
		return
	}

	// Если сообщения нет, то ожидаем его по таймеру.
	if hasTimeout {

		// Инициализируем канал, по которому получем сообщение.
		ch := make(chan string, 1)
		defer close(ch)

		// Добавляем канал в очередь на получение сообщения.
		c.pool.addToQueue(name, ch)

		timeoutTimer := time.NewTimer(timeout)

		// В зависимости что придёт раньше (таймаут или сообщение), будет обработка.
		select {
		case <-timeoutTimer.C:
			// Так как время вышло, то удаляем канал с приёма сообщения.
			c.pool.removeFromQueue(name, ch)

			w.WriteHeader(http.StatusNotFound)

		case msg := <-ch:
			// Удаляемся из очереди, так как сообщение получено.
			c.pool.removeFromQueue(name, ch)

			w.Write([]byte(msg))
		}
	}
}

// Хранилище очереди сообщений по ключу
type store struct {
	mx   sync.Mutex
	data map[string][]string
}

func newStore() *store {
	return &store{
		data: make(map[string][]string),
	}
}

// push позволяет положить ключ и сообщение в хранилище. Если сообщение уже есть,
// то новое сообщение добавится в конец.
func (s *store) push(key, msg string) {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.data[key] = append(s.data[key], msg)
}

// pop позволяет получить первый элемент ключа с удалением его из хранилища.
func (s *store) pop(key string) (string, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	values, ok := s.data[key]
	// Если нет сообщений по данному ключу.
	if !ok || len(values) == 0 {
		return "", false
	}

	// Получаем первое сообщение.
	v, newValues := popSlice(values)

	// Удаляем сообщение из хранилища.
	if len(newValues) == 0 {
		// Удаляем ключ, если больше нет элементов.
		delete(s.data, key)
	} else {
		// Убираем первый элемент из слайса сообщений.
		s.data[key] = newValues
	}

	return v, true
}

// channelPool хранилище каналов, которые принимаю сообщения для их вывода.
type channelPool struct {
	pool map[string][](chan string)
	mx   sync.Mutex
}

func NewHandlerPool() *channelPool {
	return &channelPool{
		pool: make(map[string][]chan string),
	}
}

// addToQueue добавляет канал в очередь на получение сообщения.
func (p *channelPool) addToQueue(queueName string, ch chan string) {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.pool[queueName] = append(p.pool[queueName], ch)
}

// removeFromQueue удаляет канал из очереди на получение сообщения.
func (p *channelPool) removeFromQueue(queueName string, ch chan string) {
	p.mx.Lock()
	defer p.mx.Unlock()

	new := removeItem(p.pool[queueName], ch)
	if len(new) == 0 {
		// Удаляем ключ, так как нет больше каналов для получения сообщений.
		delete(p.pool, queueName)
	} else {
		p.pool[queueName] = new
	}
}

// sendMessage служит для отправки сообщения первому каналу из очереди.
func (p *channelPool) sendMessage(key, msg string) bool {
	p.mx.Lock()
	defer p.mx.Unlock()

	handlers, ok := p.pool[key]
	if ok {
		handlers[0] <- msg
		return true
	}
	return false
}

// getQueueName возвращает имя очереди и признак его присутствия в URL.
func getQueueName(url *url.URL) (string, bool) {
	// Разбираем путь на строки, чтобы получить имя очереди. Если слайс !=2,
	// то имя очереди указано не верно или его нет.
	path := strings.SplitN(url.Path, "/", 3)
	if len(path) != 2 {
		return "", false
	}
	return path[1], true
}

//  getQueueMessage возвращает строчку сообщения и признак его присутствия в URL.
func getQueueMessage(u *url.URL) (string, bool) {
	msg := u.Query().Get("v")
	return msg, msg != ""
}

// getRequestTimeout возвращает таймаут в секундах и признак его присутствия в URL.
func getRequestTimeout(u *url.URL) (time.Duration, bool) {
	timeout := u.Query().Get("timeout")
	if timeout == "" {
		return 0, false
	}
	// Извлекаем число таймаута из строки
	t, err := strconv.ParseInt(timeout, 10, 64)
	if err != nil {
		return 0, false
	}
	// Возвращаем таймаут в секундах
	return time.Duration(t) * time.Second, true
}

// popSlice вытаскивает первый элемент из слайса.
func popSlice(s []string) (string, []string) {
	if len(s) == 0 {
		return "", nil
	}

	item := s[0]

	new := make([]string, len(s)-1)
	copy(new, s[1:])
	return item, new
}

// removeItem удаляет элемент из слайса.
func removeItem(s []chan string, item chan string) []chan string {
	new := make([]chan string, len(s)-1)

	for i, v := range s {
		if v == item {
			if len(s) == 1 {
				return nil
			} else {
				copy(new, append(s[:i], s[i+1:]...))
				return new
			}
		}
	}

	return s
}
