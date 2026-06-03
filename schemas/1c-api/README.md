# Kafka event schemas — MES Kitchen (источник: 1С)

Схемы Kafka-сообщений. 1С при сохранении объекта публикует событие `*.changed.v1` со снимком состояния в `payload`. Go-консьюмеры валидируют сообщение по соответствующей схеме и применяют изменения у себя.

## Envelope (одинаковый для всех событий)

```json
{
  "eventId":   "uuid события (для идемпотентности на стороне консьюмера)",
  "eventTime": "ISO-8601 дата-время публикации",
  "source":    "идентификатор продюсера: '1c-mes-kitchen', 'mes-api', ...",
  "payload":   { /* снимок изменившегося объекта */ }
}
```

### Зачем `source`
- **Аудит** — видно кто опубликовал странную запись
- **Защита от циклов** — когда `mes-api` начнёт публиковать те же события (после внедрения редактора ТК), консьюмер сможет игнорировать свои же сообщения (`if source == "mes-api" { skip }`)
- **Маршрутизация** — если потребуется разная обработка по источнику

### Известные значения `source`
- `1c-mes-kitchen` — 1С (сейчас единственный продюсер)
- `mes-api` — наш редактор ТК (когда появится)

## Схемы

| Файл | Топик Kafka | Объект 1С | Соответствующий HTTP-loader (старый) |
|---|---|---|---|
| `mes.orgunits.changed.v1.json`       | `mes.orgunits.changed.v1`       | Catalog_СтруктурныеЕдиницы           | org-units-loader |
| `mes.workshops.changed.v1.json`      | `mes.workshops.changed.v1`      | SS_ЦехаДляПроизводстваMES            | workshops-loader |
| `mes.baseunits.changed.v1.json`      | `mes.baseunits.changed.v1`      | Catalog_КлассификаторЕдиницИзмерения | nomenclature-loader (часть `base_units`) |
| `mes.nomenclature.changed.v1.json` | `mes.nomenclature.changed.v1` | Catalog_Номенклатура                 | nomenclature-loader (метод `nomenclature`) |
| `mes.techcards.changed.v1.json`      | `mes.techcards.changed.v1`      | Document_ТехнологическаяКарта        | tech-cards-loader |
| `mes.goodsorders.changed.v1.json`    | `mes.goodsorders.changed.v1`    | Document_ЗаказТоваров                | goods-orders-loader |

## Соглашения по именам топиков

- **Без дефисов** в имени топика (политика инфраструктуры Kafka): `orgunits`, а не `org-units`. То же правило применяется к именам JSON-файлов схем (`$id`, `title`).
- **Имя сущности в топике совпадает с именем 1С-веб-сервиса**, через который грузится та же сущность — для преемственности. Например, веб-сервис `nomenclature` → топик `mes.nomenclature.changed.v1`.
- **Имя в коде / БД может отличаться** от имени топика. Так, в нашей БД таблица `nomenclature`, а топик `mes.nomenclature.changed.v1` — это нормально, три слоя имён независимы.

## Версионирование

`v1` в имени схемы и топика. Несовместимые изменения структуры → новая версия (`v2`) и новый топик; старый топик-консьюмер продолжают работать в переходный период.

## Удаления

Передаются **флагом `deletion_mark: true`** в payload (а не отдельным топиком `*.deleted.v1`). Консьюмер при `deletion_mark=true` делает soft-delete у себя. Полная схема payload приходит и при удалении — то есть это всегда снимок состояния «как сейчас в 1С».

## Ключ Kafka-партиции

= GUID объекта (`payload.guid` или `payload.ref_key` для orgunits). Тогда все события по одному объекту попадают в одну партицию и читаются строго по порядку.
