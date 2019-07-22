package com.atguigu.gmall.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.MykafkaSender;
import com.atguigu.gmall.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDatasList;


    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDatasList = rowDatasList;
    }

    public void handle() {

        if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : columnsList) {
                    System.out.println(column.getName() + "===" + column.getValue());
                   jsonObject.put(column.getName(), column.getValue());
                }

                MykafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());


            }
        }

    }
}
