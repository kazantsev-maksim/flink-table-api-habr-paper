# Как Flink Table API упрощает разработку

Всем привет!
В этой статье мы посмотрим на два варианта решения "типичной" задачи потоковой обработки с учетом состояния при помощи фреймворка _Apache Flink_.

Если вы только начинаете знакомство с _Flink_, [здесь](https://www.youtube.com/@flinkforward) можно найти обильное количество материалов.

# Постановка задачи
Итак, давайте представим базу данных (пусть это будет _PostgreSQL_) состоящую из трех таблиц:
1. _Clients_ - общая информация по клиенту:

| Имя поля   | Тип поля         | Описание                               |
|:-----------|:-----------------|:---------------------------------------|
| clientId   | int              | уникальный идентификатор клиента       |
| name       | string           | имя клиента                            |
| surname    | string           | фамилия клиента                        |
| patronymic | optional[string] | отчество клиента (необязательное поле) |
| sex        | string           | пол                                    |

2. _ClientCompany_ - информация о компании клиента:

| Имя поля    | Тип поля | Описание                          |
|:------------|:---------|:----------------------------------|
| clientId    | int      | уникальный идентификатор клиента  |
| companyId   | int      | уникальный идентификатор компании |
| companyName | string   | название компании                 |

3. _Payment_ - информация о платежах клиента:

| Имя поля  | Тип поля  | Описание                         |
|:----------|:----------|:---------------------------------|
| clientId  | int       | уникальный идентификатор клиента |
| amount    | int       | сумма платежа                    |
| timestamp | timestamp | время свершения платежа          |

Будем получать обновления по всем таблицам через [CDC](https://ru.wikipedia.org/wiki/Захват_изменения_данных)-канал (пусть это будет [Debezium](https://debezium.io/)) в топик [Kafka](https://kafka.apache.org/) в формате [Json](https://www.json.org/json-en.html). Задачей нашей _Job_-ы будет слушать топики и выдавать обновления по клиенту (например: клиент сменил компанию) во внешнюю систему (в рамках статьи в качестве внешней системы будем использовать консоль вывода).

![Изображение](https://drive.google.com/uc?export=view&id=1JLiSPi4y_j6yDBCWOQkAYe4S3AIBh2wy "Рисунок 1 - Схема бизнесс процесса")

# Начинаем программировать

Наш технологический стек:
1. [Scala](https://docs.scala-lang.org/tour/tour-of-scala.html) v2.12.10
2. [Flink](https://nightlies.apache.org/flink/flink-docs-stable/) v1.16.0
3. [Circe](https://circe.github.io/circe/) v0.14.3

Мы разберем 2 варианта решения:
1. При помощи [Table API](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/overview/)
2. При помощи классического [State Processor API](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/libs/state_processor_api/#state-processor-api)

Прежде чем писать основную логику нам потребуется реализовать несколько общих шагов, которыми мы будем пользоваться в обеих реализациях.

Первое что нам потребуется - это научиться читать топики _Kafka_, в этом нам поможет уже готовый [Flink Kafka Connector](https://alpinegizmo.github.io/flink-docs/dev/connectors/kafka.html):

```scala
sealed trait KafkaConsumerSource {

  def configure[A: Decoder: ClassTag](bootstrapServers: String, topicName: String): KafkaSource[A] = {
    KafkaSource
      .builder()
      .setBootstrapServers(bootstrapServers)
      .setGroupId(s"$topicName-group")
      .setTopics(topicName)
      .setDeserializer(new KafkaJsonRecordDeserializer[A])
      .setStartingOffsets(OffsetsInitializer.earliest())
      .build()
  }
}

object KafkaConsumerSource extends KafkaConsumerSource {

  def configureKafkaDataSource[A: Decoder: ClassTag](
      topicName: String
  )(implicit env: StreamExecutionEnvironment,
    typeInformation: TypeInformation[A]): DataStream[A] = {
    val kafkaSource = KafkaConsumerSource.configure[A](Kafka.BootstrapServers, topicName)
    env.fromSource[A](kafkaSource, WatermarkStrategy.noWatermarks[A](), s"$topicName-flow")
  }
}

class KafkaJsonRecordDeserializer[A: Decoder: ClassTag] extends KafkaRecordDeserializationSchema[A] {

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[A]): Unit = {
    val value = new String(record.value())
    val obj   = Serde.toObj(value).toOption // в случае неудачи при десериализации, ошибка просто игнорируется и событие отбрасывается (в реальных проектах так делать не стоит)
    obj.foreach(out.collect)
  }

  override def getProducedType: TypeInformation[A] = {
    TypeExtractor
      .getForClass(classTag[A].runtimeClass.asInstanceOf[Class[A]])
      .asInstanceOf[TypeInformation[A]]
  }
}
```
В сериализации/десериализации сообщений нам поможет _Circe_:

```scala
object Serde {

  def toObj[A: Decoder](json: String): Either[circe.Error, A] = decode[A](json)

  def toJson[A: Encoder](obj: A): String = obj.asJson.toString()
}
```
Мы немного упростим себе задачу, сымитируем поведение _CDC_-канала, отправляя сообщения требуемого формата напрямую в топик _Kafka_:

```scala
case class Client (before: Option[Client.Value], after: Option[Client.Value], operation: String)

object Client {
  case class Value(clientId: Int, name: String, surname: String, patronymic: Option[String], sex: String)
}

case class ClientCompany (before: Option[ClientCompany.Value], after: Option[ClientCompany.Value], operation: String)

object ClientCompany {
  case class Value(clientId: Int, companyId: Int, companyName: String)
}

case class Payment (before: Option[Payment.Value], after: Option[Payment.Value], operation: String)

object Payment {
  case class Value(clientId: Int, amount: Int, timestamp: Instant)
}

```
Теперь мы готовы приступить к основной части.

# Использование Flink Table API

Итак, прежде всего нам нужно получить необходимое окружение, поскольку мы будем работать в потоковом режиме, заручимся таковым:

```scala
implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
```
Далее получим окружение для доступа к _Table API_ :

```scala
implicit val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
```

Формируем _Kafka Source_ при помощи [кода](#Начинаем-программировать) который мы подготовили выше:

```scala
val clients: DataStream[Client] = KafkaConsumerSource.configureKafkaDataSource[Client]("clients")
```
Теперь нам остался всего один шаг до получения представления с которым нам предстоит работать - [Dynamic Tables](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/concepts/dynamic_tables/).  Чтобы сделать этот шаг максимально простым, воспользуемся возможностями [Scala](https://docs.scala-lang.org/overviews/core/implicit-classes.html) и расширим _DataStream_ методом _toStreamTable_:

```scala
implicit class DataStreamOps[A: RowTransformer](val stream: DataStream[A]) {

  def toStreamTable(primaryKey: String*)(implicit env: StreamTableEnvironment): Table = {
    val transformer = RowTransformer[A]
    stream
      .map(transformer.toRow(_))
      .flatMap(_.toSeq)(transformer.typeInformation)
      .toChangelogTable(
        env,
        Schema
          .newBuilder()
          .primaryKey(primaryKey: _*)
          .build(),
        ChangelogMode.upsert()
      )
  }
}
```
На вход функции подается список имен полей, которые составят первичный ключ нашей таблицы. Как и в большинстве фреймворков наименьшей единицей для работы с таблицами является строка ([Row](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/types/Row.html)). Нужен способ преобразования всех наших сущностей в тип _Row_, продолжим пользоваться возможностями _Scala_, на этот раз тем что _Scala_ позволяет писать код в [функциональном стиле](https://ru.wikipedia.org/wiki/Функциональное_программирование), а конкретно мы воспользуемся [Type Class](https://habr.com/ru/companies/tinkoff/articles/147759/)-ми. Наш _Type Class_ будет иметь следующий вид:

```scala
sealed trait RowTransformer[A] {
  def toRow(entity: A): Option[Row]
  protected val fieldsName: Array[String]
  implicit def typeInformation: TypeInformation[Row]
}
```
Представим реализацию для одной из таблиц (для остальных алгоритм будет аналогичен):
```scala
implicit case object ClientRow extends RowTransformer[Client] {
  override def toRow(entity: Client): Option[Row] = {
    val kind   = kindOf(entity.operation)
    val record = entity.before.orElse(entity.after)
    record.map(r => Row.ofKind(kind, Int.box(r.clientId), r.name, r.surname, r.patronymic.orNull, r.sex))
  }

  override implicit def typeInformation: TypeInformation[Row] = {
    Types.ROW_NAMED(fieldsName, Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)
  }

  override protected val fieldsName: Array[String] = Array("clientId", "name", "surname", "patronymic", "sex")
}
```
Теперь мы можем преобразовывать `DataStream` в `Table` одним действием:
```scala
val clients: Table = KafkaConsumerSource.configureKafkaDataSource[Client]("clients")
  .toStreamTable("clientId")
```
Мы окончательно подготовились к написанию основной логики. Для нашей задачи будем использовать _inner join_, т.е. при получении нового события мы будем выдавать результат во _внешнюю систему_ только в случае наличия данных по ключу во всех таблицах.

```scala
clients
  .join(companies, $"client.clientId" === $"company.clientId")
  .join(payments, $"client.clientId" === $"payment.clientId")
  .select("client.clientId", "name", "surname", "companyId", "companyName", "amount", "timestamp")
  .toChangelogStream(Schema.derived(), ChangelogMode.upsert())
  .print("Portfolio")
```

![Изображение](https://drive.google.com/uc?export=view&id=1pA-PgVOeznPgjkI3lQYJkJtih-Hb2CoO "Рисунок 2 - Dynamic Tables")

# Использование State Processor API

Снова начнем с окружения, в этом варианте нам будет достаточно только окружения для работы в потоковом режиме:

```scala
implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
```

В прошлом варианте мы приводили все наши сущности к типу _Table_, сейчас нам нужно сделать нечто похожее:

```scala
case class PortfolioState(
   id: Int,
   client: Option[Client.Value],
   company: Option[ClientCompany.Value],
   payment: Option[Payment.Value]) {

  def complete: Option[Portfolio] = {
    for {
      client  <- client
      company <- company
      payment <- payment
    } yield {
      Portfolio(
        client.clientId,
        client.name,
        client.surname,
        company.companyId,
        company.companyName,
        payment.amount,
        payment.timestamp
      )
    }
  }
}
```
Мы описали свой собственный аналог таблицы, добавив в него метод _complete_ формирующий результат только в случае наличия данных по всем трем сущностям (аналог _inner join_). Опишем способ трансформации исходной сущности в наш тип (для остальных сущностей будет аналогично):

```scala
object PortfolioState {

  def apply(client: Client): PortfolioState = {
    val id = client.before.map(_.clientId).getOrElse(client.after.map(_.clientId).get)
    PortfolioState(id, client.before.orElse(client.after), None, None)
  }
}
```
Итоговый результат:
```scala
val clients: DataStream[PortfolioState] = KafkaConsumerSource.configureKafkaDataSource[Client]("clients").map(PortfolioState(_))
```
Последнее чего нам не хватает - это [обработчика](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/state_processor_api/#keyed-state) событий, который будет поддерживать актуальное состояние и выдавать результаты:

```scala
class StateProcessor extends KeyedProcessFunction[Int, PortfolioState, Portfolio] {

  private var state: ValueState[PortfolioState] = _

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(
      new ValueStateDescriptor[PortfolioState]("state", createTypeInformation[PortfolioState])
    )
  }

  override def processElement(
     value: PortfolioState,
     ctx: KeyedProcessFunction[Int, PortfolioState, Portfolio]#Context,
     ut: Collector[Portfolio]
  ): Unit = {
    Option(state.value()).fold {
      state.update(value) // если state == null, инициализируем его входным значением
    } { currentState =>
      val updatedState = currentState.copy(
        client = value.client.orElse(currentState.client),
        company = value.company.orElse(currentState.company),
        payment = value.payment.orElse(currentState.payment)
      )
      updatedState.complete.foreach(out.collect) // если данных хватает, то выдаем результат
      state.update(updatedState)
    }
  }
}
```
Соберем все вместе:
```scala
clients
  .union(companies) // объединяем все три потока в один
  .union(payments)
  .keyBy(_.id) // шардиурем данные по полю id (clientId)
  .process(new PortfolioStateProcessor())
  .print("Portfolio")
```

# Итоги

Мы рассмотрели 2 разных подхода, даже несмотря на всю простоту постановки подход с применением _Table API_ для задач отслеживания изменений и моментальной выдачи результат выглядит привлекательнее, мы получаем возможность описывать желаемый результат на привычном _SQL_-подобном синтаксисе находясь в бесконечном потоке событий.

Полный код проекта можно найти [здесь](https://github.com/way-tendoo/flink-table-api-habr-paper)