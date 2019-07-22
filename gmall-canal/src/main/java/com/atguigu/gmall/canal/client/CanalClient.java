package com.atguigu.gmall.canal.client;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall.canal.handler.CanalHandler;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall0218.*");
            Message message = canalConnector.get(100);

            if (message.getEntries().size() == 0) {
                System.out.println("休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {

                        CanalEntry.RowChange rowChange = null;
                        ByteString storeValue = entry.getStoreValue();
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        String tableName = entry.getHeader().getTableName();

                        CanalHandler handler = new CanalHandler(tableName, eventType, rowDatasList);

                        handler.handle();

                    }
                }


            }
        }







    }
}
