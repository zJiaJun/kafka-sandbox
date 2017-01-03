package com.github.zjiajun.kafka.stream;

import com.github.zjiajun.kafka.stream.model.Item;
import com.github.zjiajun.kafka.stream.model.Order;
import com.github.zjiajun.kafka.stream.model.User;
import com.github.zjiajun.kafka.stream.serdes.SerdesFactory;
import com.github.zjiajun.kafka.stream.timeextractor.OrderTimestampExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.util.Properties;

public class PurchaseAnalysis {

	/**
	 * 算出用户与商品同地址的订单中，男女分别总共花了多少钱的基础上，算出不同地区（用户地址），
	 * 不同性别的订单数及商品总数和总金额。输出结果schema如下
	 地区（用户地区，如SH），性别，订单总数，商品总数，总金额
	 示例输出
	 SH, male, 3, 4, 188888.88
	 BJ, femail, 5, 8, 288888.88
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-purchase-analysis2");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "127.0.0.1:2181/kafka0.10.1.0");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

		KStreamBuilder streamBuilder = new KStreamBuilder();
		KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");
		KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");
		KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");
//		itemTable.toStream().foreach((String itemName, Item item) -> System.out.printf("Item info %s-%s-%s-%s\n", item.getItemName(), item.getAddress(), item.getType(), item.getPrice()));

		KStream<String, OrderUser> kStream = orderStream.leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class));
		KStream<String, GroupInfo> stringOrderUserItemKStream = kStream.filter((String name, OrderUser orderUser) -> orderUser.userAddress != null)
				.map((String name, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))
				.through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class),
						(String key, OrderUser orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
				.leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
				.filter((String item, OrderUserItem orderUserItem) -> StringUtils.compare(orderUserItem.userAddress, orderUserItem.itemAddress) == 0)
				.map((String itemName,OrderUserItem orderUserItem)->  KeyValue.pair(orderUserItem.userAddress+","+orderUserItem.gender,
						new GroupInfo(1,orderUserItem.quantity,orderUserItem.quantity * orderUserItem.itemPrice)));


		KTable<String, GroupInfo> kTable = stringOrderUserItemKStream.groupByKey(Serdes.String(), SerdesFactory.serdFrom(GroupInfo.class))
				.reduce((GroupInfo g1, GroupInfo g2) -> {
					GroupInfo g3 = new GroupInfo();
					g3.setItemCount(g1.itemCount + g2.itemCount);
					g3.setAmountSum(g1.amountSum + g2.amountSum);
					g3.setOrderCount(g1.orderCount + g2.orderCount);
					return g3;

				}, "gender-amount-state-store");

		kTable.toStream().map((String addGen,GroupInfo g)-> KeyValue.pair(addGen,addGen+"_"+g.getOrderCount()+"_"+g.getItemCount()+"_"+g.getAmountSum()))
				.foreach((s1,s2)-> System.out.println(s1+"****"+s2));

		stringOrderUserItemKStream.foreach((item,orderUserItem)-> System.out.println(item+":"+orderUserItem));


		/*
		KTable<String, Double> kTable = orderStream
				.leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class))
				.filter((String userName, OrderUser orderUser) -> orderUser.userAddress != null)
				.map((String userName, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))
				.through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class), (String key, OrderUser orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
				.leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
				.filter((String item, OrderUserItem orderUserItem) -> StringUtils.compare(orderUserItem.userAddress, orderUserItem.itemAddress) == 0)
//				.foreach((String itemName, OrderUserItem orderUserItem) -> System.out.printf("%s-%s-%s-%s\n", itemName, orderUserItem.itemAddress, orderUserItem.userName, orderUserItem.userAddress))
				.map((String item, OrderUserItem orderUserItem) -> KeyValue.<String, Double>pair(orderUserItem.gender, (Double)(orderUserItem.quantity * orderUserItem.itemPrice)))
				.groupByKey(Serdes.String(), Serdes.Double())
				.reduce((Double v1, Double v2) -> v1 + v2, "gender-amount-state-store");
//		kTable.foreach((str, dou) -> System.out.printf("%s-%s\n", str, dou));
		kTable
			.toStream()
			.map((String gender, Double total) -> new KeyValue<String, String>(gender, String.valueOf(total)))
			.to("gender-amount");
			*/

		
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		
		System.in.read();
		kafkaStreams.close();
		kafkaStreams.cleanUp();
	}



	public static class GroupInfo {
		private int orderCount;
		private int itemCount;
		private double amountSum;

		private GroupInfo() {

		}

		@Override
		public String toString() {
			return "GroupInfo{" +
					"orderCount=" + orderCount +
					", itemCount=" + itemCount +
					", amountSum=" + amountSum +
					'}';
		}

		private GroupInfo(int orderCount, int itemCount, double amountSum) {
			this.orderCount = orderCount;
			this.itemCount = itemCount;
			this.amountSum = amountSum;
		}

		public int getOrderCount() {
			return orderCount;
		}

		public void setOrderCount(int orderCount) {
			this.orderCount = orderCount;
		}

		public int getItemCount() {
			return itemCount;
		}

		public void setItemCount(int itemCount) {
			this.itemCount = itemCount;
		}

		public double getAmountSum() {
			return amountSum;
		}

		public void setAmountSum(double amountSum) {
			this.amountSum = amountSum;
		}
	}



		public static class OrderUser {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		
		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public static OrderUser fromOrder(Order order) {
			OrderUser orderUser = new OrderUser();
			if(order == null) {
				return orderUser;
			}
			orderUser.userName = order.getUserName();
			orderUser.itemName = order.getItemName();
			orderUser.transactionDate = order.getTransactionDate();
			orderUser.quantity = order.getQuantity();
			return orderUser;
		}
		
		public static OrderUser fromOrderUser(Order order, User user) {
			OrderUser orderUser = fromOrder(order);
			if(user == null) {
				return orderUser;
			}
			orderUser.gender = user.getGender();
			orderUser.age = user.getAge();
			orderUser.userAddress = user.getAddress();
			return orderUser;
		}

		@Override
		public String toString() {
			return "OrderUser{" +
					"userName='" + userName + '\'' +
					", itemName='" + itemName + '\'' +
					", transactionDate=" + transactionDate +
					", quantity=" + quantity +
					", userAddress='" + userAddress + '\'' +
					", gender='" + gender + '\'' +
					", age=" + age +
					'}';
		}
	}
	
	public static class OrderUserItem {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		private String itemAddress;
		private String itemType;
		private double itemPrice;
		
		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getItemAddress() {
			return itemAddress;
		}

		public void setItemAddress(String itemAddress) {
			this.itemAddress = itemAddress;
		}

		public String getItemType() {
			return itemType;
		}

		public void setItemType(String itemType) {
			this.itemType = itemType;
		}

		public double getItemPrice() {
			return itemPrice;
		}

		public void setItemPrice(double itemPrice) {
			this.itemPrice = itemPrice;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser) {
			OrderUserItem orderUserItem = new OrderUserItem();
			if(orderUser == null) {
				return orderUserItem;
			}
			orderUserItem.userName = orderUser.userName;
			orderUserItem.itemName = orderUser.itemName;
			orderUserItem.transactionDate = orderUser.transactionDate;
			orderUserItem.quantity = orderUser.quantity;
			orderUserItem.userAddress = orderUser.userAddress;
			orderUserItem.gender = orderUser.gender;
			orderUserItem.age = orderUser.age;
			return orderUserItem;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser, Item item) {
			OrderUserItem orderUserItem = fromOrderUser(orderUser);
			if(item == null) {
				return orderUserItem;
			}
			orderUserItem.itemAddress = item.getAddress();
			orderUserItem.itemType = item.getType();
			orderUserItem.itemPrice = item.getPrice();
			return orderUserItem;
		}

		@Override
		public String toString() {
			return "OrderUserItem{" +
					"userName='" + userName + '\'' +
					", itemName='" + itemName + '\'' +
					", transactionDate=" + transactionDate +
					", quantity=" + quantity +
					", userAddress='" + userAddress + '\'' +
					", gender='" + gender + '\'' +
					", age=" + age +
					", itemAddress='" + itemAddress + '\'' +
					", itemType='" + itemType + '\'' +
					", itemPrice=" + itemPrice +
					'}';
		}
	}

}
