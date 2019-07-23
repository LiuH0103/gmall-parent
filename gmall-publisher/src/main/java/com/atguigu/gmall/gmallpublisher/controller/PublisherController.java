package com.atguigu.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealTimeTotal(@RequestParam("date") String date){

        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        int dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value", dauTotal);
        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealTimeHour(@RequestParam("id") String id,@RequestParam("date") String date ){
        if (id.equals("dau")) {
            Map todayMap = publisherService.getDauHour(date);
            String ydString= "";
            try {
                 ydString = getYDString(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Map yesDayMap = publisherService.getDauHour(ydString);

            Map<String, Map> hourMap = new HashMap<>();
            hourMap.put("yesterday", yesDayMap);
            hourMap.put("today", todayMap);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }


    private String getYDString(String today) throws ParseException {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = dateFormat.parse(today);
        Date yesDay = DateUtils.addDays(date, -1);
        String yesDayString = dateFormat.format(yesDay);

        return yesDayString;
    }


}

