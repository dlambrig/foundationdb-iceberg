import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import java.util.function.Function;

public class Demo {
    private enum FlowerType {
        ROSE,
        TULIP,
        LILY,
    }

    private static RecordLayerDemoProto.Flower buildFlower(FlowerType type, RecordLayerDemoProto.Color color) {
        return RecordLayerDemoProto.Flower.newBuilder()
                .setType(type.name())
                .setColor(color)
                .build();
    }

    public static void main(String[] args) {
        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();
        // Define the keyspace for our application
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("record-layer-demo", KeySpaceDirectory.KeyType.STRING, "record-layer-demo"));
        // Get the path where our record store will be rooted
        KeySpacePath path = keySpace.path("record-layer-demo");
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(RecordLayerDemoProto.getDescriptor());
        metaDataBuilder.getRecordType("Order")
                .setPrimaryKey(Key.Expressions.field("order_id"));
        metaDataBuilder.addIndex("Order", new Index("priceIndex", Key.Expressions.field("price")));
        RecordMetaData recordMetaData = metaDataBuilder.build();
        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetaData)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();
        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);

            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(1)
                    .setPrice(123)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(23)
                    .setPrice(34)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.PINK))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(3)
                    .setPrice(55)
                    .setFlower(buildFlower(FlowerType.TULIP, RecordLayerDemoProto.Color.YELLOW))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(100)
                    .setPrice(9)
                    .setFlower(buildFlower(FlowerType.LILY, RecordLayerDemoProto.Color.RED))
                    .build());

            return null;
        });
        FDBStoredRecord<Message> storedRecord = db.run(context ->
                // load the record
                recordStoreProvider.apply(context).loadRecord(Tuple.from(1))
        );
        assert storedRecord != null;

        // a record that doesn't exist is null
        FDBStoredRecord<Message> shouldNotExist = db.run(context ->
                recordStoreProvider.apply(context).loadRecord(Tuple.from(99999))
        );
        assert shouldNotExist == null;
        RecordLayerDemoProto.Order order = RecordLayerDemoProto.Order.newBuilder()
                .mergeFrom(storedRecord.getRecord())
                .build();
        System.out.println(order);
    }
}
