package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceimpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public int getDauTotal(String date) {
        int dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHourList = dauMapper.getDauHour(date);
        Map dauHourMap = new HashMap();
        for (Map map : dauHourList) {
            String loghour =(String) map.get("LOGHOUR");
            long ct = (long)map.get("CT");
            dauHourMap.put(loghour,ct);
        }
        return dauHourMap;
    }
}
