<!-- If you are an AI agent, please read agents.md -->

<div align="center">

<img src="docs/asset/westand.svg" width="250" height="250">

<br>

<img src="https://github.com/openlibrecommunity/material/blob/master/olcrtc.png" width="250" height="250">

<br>
<br>

<img src="https://count.owenewans.org/openlibrecommunity/olcrtc?theme=moebooru&notitle">

</div>

# olcRTC

**RU** / [EN](readme.md)


`olcRTC` (OpenLibreCommunity RTC) - зашифрованный TCP-over-WebRTC туннель. Трафик маскируется под обычный видеозвонок на разрешённых сервисах (Jitsi, Yandex Telemost, WbStream). Внутри - шифрование XChaCha20-Poly1305 и мультиплексирование smux поверх WebRTC data/video каналов.

Статус: **Beta**

```text
app -> SOCKS5 -> olcrtc cnc -> WebRTC/SFU сервис -> olcrtc srv -> интернет
```

> **Важно:** проверяйте, что нужный сервис видеозвонков есть в белых списках и работает в вашей сети. Если нет - используйте другой.

## Этот форк

Это форк [openlibrecommunity/olcrtc](https://github.com/openlibrecommunity/olcrtc),
который делает ProofKit. Работа идёт в ветке **`proofkit`**, её перекладывают на
upstream по мере его движения; `master` здесь - нетронутая копия upstream и
ничего из перечисленного не содержит. Именно эту ветку линкует клиент ProofKit
([romanpodpriatov/olcbox](https://github.com/romanpodpriatov/olcbox)) на всех
платформах - для этого почти всё ниже и сделано: платный релей должен отличать
своих пользователей друг от друга, а телефон - не сервер.

| Добавлено здесь | Что это |
| --- | --- |
| Кольцо серверных ключей | `crypto.keys` / `crypto.keys_file`. Сервер держит несколько ключей и закрепляет за пиром тот, под которым аутентифицировалась его первая запись, - так у каждого клиента может быть свой ключ. Одиночный `crypto.key` - кольцо из одной записи и ведёт себя как раньше; клиенты не меняются. |
| Учёт трафика по ключам | `stats.listen` отдаёт `GET /stats` на loopback с побайтовыми итогами по каждому ключу. Этого хватает, чтобы выставить счёт или отключить ключ, и не нужен никакой control plane. |
| Дейтаграммная полоса с потерями | Дейтаграммы идут рядом с байтовым потоком на `vp8channel`, `datachannel` и livekit, так что UDP-потоку больше не нужно притворяться стримом: vp8channel помечает их `OLUD`/`OLUB` и отправляет после control-кадров и перед данными KCP, livekit публикует их ненадёжно в своём топике. |
| SOCKS5 UDP ASSOCIATE | `udp.enabled` пускает через релей звонки, игры и всё остальное, что является дейтаграммами, по этой полосе, с `udp.max_flows` на сторону. Выключено, пока не попросят. |
| DNS - мимо полосы с потерями | За tun2socks каждый пакет телефона приходит как UDP-ассоциация, запросы резолвера в том числе, а потерянный под нагрузкой запрос - это застывшая страница. Дейтаграмма на порт 53 теперь идёт smux-стримом как TCP DNS (RFC 7766): 64 в полёте, по пять секунд каждый, полоса - запасной путь. |
| Кольцо резолверов на мобильных | Имена резолвятся через защищённые сокеты хоста и список резолверов сессии; молчащий понижается, а не ждётся. Внутри туннеля системный резолвер - это сам туннель, а оператор, который блэкхолит публичный, встречается нередко. |
| Потолок памяти, который задаёт хост | `mobile.SetMemoryLimit`, `MemoryLimit`, `MemoryStats`, `FreeOSMemory`, `GoroutineSummary` и писатель логов. Расширению packet tunnel на iOS достаётся около 50 МБ на всё; Go-рантайм, выбирающий потолок сам по объёму памяти устройства, переступает эту границу за одну сборку мусора. |
| Облегчённый мобильный бинд | С `-tags olcrtc_lean` из gomobile-сборки выпадает транспорт videochannel - таблицы кодировок QR строятся в init ради транспорта, который телефон не выбирает. Движок livekit остаётся в любой сборке: его называет auth-провайдер WB Stream, и без него облегчённый бинд ронял сессию сразу после гостевого токена. Обычная сборка и CLI не меняются. |
| Буферы по размеру телефона | Окна KCP, очереди пакетов, история NACK, чтения трека и буферы чтения на ассоциацию рассчитаны на телефон, а не на сервер, и выбираются профилем хоста, а не прибиты в коде. |

Кроме этого: клиент отличает старый пир, неверный ключ и пустую комнату друг от
друга, а не отваливается по таймауту на всех трёх; нумерация записей и
replay-проверка ведутся по каждой полосе отдельно; очередь отправки в бридж
Jitsi притормаживается вместо провала отправки; соединение устанавливается на
обе адресные семьи с повтором, когда маршрута не нашлось, - это нужно и
IPv6-only оператору, и NAT64 в App Review.

Изменения, чьё место в upstream, оформляются туда pull request'ами - ветки
`pr/*` здесь как раз они, по одному изменению в каждой.

## Возможности

- **Провайдеры:** `jitsi`, `telemost`, `wbstream`
- **Транспорты:** `datachannel`, `vp8channel`, `seichannel`, `videochannel`
- **Платформы:** Linux, macOS, Windows, Android (gomobile), встраиваемая Go-библиотека
- **Публичные Go-пакеты:** `pkg/olcrtc/client`, `pkg/olcrtc/tunnel`, `pkg/olcrtc/engineconn`

Рекомендуемый старт: `jitsi + datachannel`.

Текущие сборки используют OLC2-шифрование с направленными ключами HKDF-SHA256, отдельным AAD для data/control и replay-защитой. Fallback на старый crypto format отсутствует. `seichannel` и `videochannel` используют OLVC версии 5 и отклоняют старые видеокадры. Обновляй обе стороны одновременно.

Словари display name встроены в бинарник. Необязательное поле YAML `data` может указать каталог с файлами `names` и `surnames` для их замены.

## Установка в один клик

```sh
curl -fsSL https://raw.githubusercontent.com/romanpodpriatov/olcrtc/proofkit/install.sh | bash
```

Ставит Podman, если его нет, клонирует ветку `proofkit` этого форка, собирает бинарник в контейнере, задаёт несколько вопросов (сервер или клиент, провайдер, транспорт, комната, ключ) и запускает. Запусти скрипт один раз на сервере (режим `srv`) и один раз на клиенте (режим `cnc`) - им нужны одинаковые room ID и ключ шифрования.

Если репозиторий уже склонирован, просто запусти `./install.sh`.

Полные инструкции в [docs/fast.md](docs/fast.ru.md) и [docs/manual.md](docs/manual.ru.md).

## Документация

- [about.md](docs/about.ru.md) - архитектура, провайдеры, транспорты, публичный API
- [fast.md](docs/fast.ru.md) - быстрый старт для новичков
- [manual.md](docs/manual.ru.md) - ручная сборка
- [configuration.md](docs/configuration.ru.md) - настройка YAML
- [settings.md](docs/settings.ru.md) - матрица совместимости
- [uri.md](docs/uri.ru.md) - формат URI клиента
- [sub.md](docs/sub.ru.md) - формат подписки
- [gate.md](docs/gate.ru.md) - release gate: туннель под нагрузкой на настоящих relay и его отчёт

## Сборка

```sh
mage build   # текущая платформа
mage cross   # кросс-компиляция
mage test    # тесты
mage lint    # golangci-lint
mage mobile  # gomobile bindings (Android)
```

## Клиенты

- Основной клиент:
  - [owenewans/owenclave](https://github.com/owenewans/owenclave) - Android-клиент прокси (форк exclave). Поддерживает все распространённые протоколы (vless, hysteria2, mieru, trojan, vmess, tuic, shadowsocks, socks ...) плюс `olcrtc`, формат URI `olcrtc://` и подписки
- Клиент этого форка:
  - [romanpodpriatov/olcbox](https://github.com/romanpodpriatov/olcbox) - ProofKit, форк olcbox из списка ниже. Kotlin Multiplatform/Compose для Android, iOS, macOS, Windows и Linux; olcRTC рядом с VLESS Reality, VLESS поверх TLS, Hysteria2 и XHTTP - тот клиент, ради которого сделано всё перечисленное выше
- Клиенты сообщества:
  - [venterum/veil](https://github.com/venterum/veil) - V2Ray/Xray клиент для Android (форк v2rayNG), Material 3. Протоколы: VMess, VLESS, Shadowsocks, Trojan, SOCKS, WireGuard, Hysteria2 + `olcrtc`
  - [alananisimov/olcbox](https://github.com/alananisimov/olcbox) - Мультиплатформенный UI-клиент (Android, iOS, macOS, Windows, Linux). Kotlin Multiplatform/Compose. Все провайдеры (Jitsi, Telemost, WB Stream, Jazz), все транспорты, split tunneling, режимы TUN/proxy

## Сообщество

- Telegram: [@openlibrecommunity](https://t.me/openlibrecommunity)
- Issues: [github.com/openlibrecommunity/olcrtc/issues](https://github.com/openlibrecommunity/olcrtc/issues)

## Лицензия

WTFPL

<div align="center">

---

Telegram: [zarazaex](https://t.me/zarazaexe)
<br>
Email: [zarazaex@tuta.io](mailto:zarazaex@tuta.io)
<br>
Site: [zarazaex.xyz](https://zarazaex.xyz)

</div>
