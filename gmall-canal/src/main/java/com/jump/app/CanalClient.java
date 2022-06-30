package com.jump.app;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jump.constants.GmallConstants;
import com.jump.utils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @ClassName CanalClient
 * @Description
 * @Author Four5prings
 * @Date 2022/6/30 21:39
 * @Version 1.0
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //创建连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "", "", "");
        while (true) {
            canalConnector.subscribe("gmall2022.*");

            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            //健壮性判断 如果没有数据，线程等待
            if (entries.size() <= 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表名
                    String tableName = entry.getHeader().getTableName();

                    //Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    if (entryType.equals(CanalEntry.EntryType.ROWDATA)) {
                        //序列化
                        ByteString storeValue = entry.getStoreValue();

                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //TODO 根据条件获取数据
                        handler(tableName, eventType, rowDatasList);
                    }
                }

            }
        }

    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取那个表的数据
        if ("order_info".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toJSONString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toString());
            }
        }
    }
}
