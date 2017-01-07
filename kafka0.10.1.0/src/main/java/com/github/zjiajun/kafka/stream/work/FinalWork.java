package com.github.zjiajun.kafka.stream.work;

import com.github.zjiajun.kafka.stream.model.Item;
import com.github.zjiajun.kafka.stream.model.Order;
import com.github.zjiajun.kafka.stream.model.User;
import com.github.zjiajun.kafka.stream.serdes.SerdesFactory;
import com.github.zjiajun.kafka.stream.timeextractor.OrderTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.*;


/**
 * @author zhujiajun
 * @since 2017/1/7
 */
public class FinalWork {

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "final-work");
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
        KStream<Windowed<String>, GroupInfo> kStream = orderStream
                .leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class))
                .filter((String userName, OrderUser orderUser) -> (orderUser.userAddress != null && (orderUser.getAge() >= 18 &&  orderUser.getAge() <= 35 )))
                .map((String userName, OrderUser orderUser) -> new KeyValue<String, OrderUser>(orderUser.itemName, orderUser))
                .through(Serdes.String(), SerdesFactory.serdFrom(OrderUser.class), (String key, OrderUser orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
                .leftJoin(itemTable, (OrderUser orderUser, Item item) -> OrderUserItem.fromOrderUser(orderUser, item), Serdes.String(), SerdesFactory.serdFrom(OrderUser.class))
                .map((String item, OrderUserItem orderUserItem) -> KeyValue.<String, GroupInfo>pair((orderUserItem.itemType),
                        new GroupInfo(orderUserItem.itemType, new ArrayList<GroupItemInfo>(){
                            private static final long serialVersionUID = 1L;
                            {
                                add(new GroupItemInfo(orderUserItem.transactionDate, orderUserItem.itemName, orderUserItem.quantity, orderUserItem.itemPrice,  (orderUserItem.quantity * orderUserItem.itemPrice)));
                            }})
                ))
                .groupByKey(Serdes.String(), SerdesFactory.serdFrom(GroupInfo.class))
                .reduce((GroupInfo v1, GroupInfo v2) -> {
                            GroupInfo v3 = new GroupInfo(v1.getItemType());
                            List<GroupItemInfo> newItemlist = new ArrayList<GroupItemInfo>();
                            newItemlist.addAll(v1.getItemList());
                            newItemlist.addAll(v2.getItemList());
                            v3.setItemList(newItemlist);
                            return v3;
                        }
                        , TimeWindows.of(1000 * 5).advanceBy(1000 * 5)
                        , "gender-amount-state-store").toStream();
        kStream.map((Windowed<String> window, GroupInfo groupInfo) -> {
            return new KeyValue<String, String>(window.key(),  groupInfo.printTop10(window.window().start(), window.window().end()));
        }).to(Serdes.String(), Serdes.String(), "gender-amount");
        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        System.in.read();
        kafkaStreams.close();
        kafkaStreams.cleanUp();
    }
    public static class GroupInfo{
        private String itemType;
        private List<GroupItemInfo> itemList;

        public GroupInfo(){

        }
        public GroupInfo(String itemType){
            this.itemType = itemType;
        }
        public GroupInfo(String itemType, List<GroupItemInfo> itemList){
            this.itemType = itemType;
            this.itemList = itemList;
        }
        public String getItemType() {
            return itemType;
        }
        public void setItemType(String itemType) {
            this.itemType = itemType;
        }
        public List<GroupItemInfo> getItemList() {
            return itemList;
        }
        public void setItemList(List<GroupItemInfo> itemList) {
            this.itemList = itemList;
        }
        /**
         * 根据金额汇总倒序
         */
        private List<GroupItemInfo> sortBySumDesc(Collection<GroupItemInfo> allItems){
            List<GroupItemInfo> result = new ArrayList<GroupItemInfo>();
            result.addAll(allItems);
            result.sort((o1, o2) -> {
                if (o1.getSum() == o2.getSum()) {
                    return 0;
                } else if (o1.getSum() > o2.getSum()) {
                    return -1;
                } else {
                    return 1;
                }
            });
            return result;
        }
        /**
         * 找回前10名
         */
        public String printTop10(long startDate, long endDate){
            double allAmount = 0.0;
            Map<String, GroupItemInfo> groupMap = new HashMap<String, GroupItemInfo>();
            for(GroupItemInfo item : itemList){
                String key = item.getItemName();
                allAmount += item.getSum();
                if(groupMap.containsKey(key)){
                    GroupItemInfo oldItem = groupMap.get(key);
                    oldItem.setCount(oldItem.getCount() + item.getCount());
                    oldItem.setSum(oldItem.getSum() + item.getSum());
                }else{
                    groupMap.put(key, item);
                }
            }
            List<GroupItemInfo> sortedResult = sortBySumDesc(groupMap.values());
            StringBuilder sb =  new StringBuilder();
            for(int i = 1; i <= 10 ; i++){
                if(sortedResult.size() >= i && sortedResult.get(i-1) != null){
                    GroupItemInfo oneItem = sortedResult.get(i-1);
                    sb.append(startDate).append(",").append(endDate).append(",").append(itemType).append(",").append(oneItem.getItemName()).append(",")
                            .append(oneItem.getCount()).append(",").append(oneItem.getPrice()).append(",").append(oneItem.getSum()).append(",").append(allAmount)
                            .append(",").append(i).append("\n");
                }else{
                    break;
                }
            }
            return sb.toString();
        }
    }

    private static class GroupItemInfo{
        private long transactionDate;
        private String itemName;
        private int count;
        private double price;
        private double sum;
        public GroupItemInfo(){

        }
        public GroupItemInfo(long transactionDate, String itemName, int count, double price,
                             double sum) {
            this.transactionDate = transactionDate;
            this.itemName = itemName;
            this.count = count;
            this.price = price;
            this.sum = sum;
        }
        public long getTransactionDate() {
            return transactionDate;
        }
        public void setTransactionDate(long transactionDate) {
            this.transactionDate = transactionDate;
        }
        public String getItemName() {
            return itemName;
        }
        public void setItemName(String itemName) {
            this.itemName = itemName;
        }
        public int getCount() {
            return count;
        }
        public void setCount(int count) {
            this.count = count;
        }
        public double getPrice() {
            return price;
        }
        public void setPrice(double price) {
            this.price = price;
        }
        public double getSum() {
            return sum;
        }
        public void setSum(double sum) {
            this.sum = sum;
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
    }

}

