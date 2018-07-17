package com.sogoanalyze.model;

import java.io.Serializable;

public class SogoBean implements Serializable {
    private String time;
    private String uid;
    private String content;
    private String count_order;
    private String count_click;
    private String url;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getCount_order() {
        return count_order;
    }

    public void setCount_order(String count_order) {
        this.count_order = count_order;
    }

    public String getCount_click() {
        return count_click;
    }

    public void setCount_click(String count_click) {
        this.count_click = count_click;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
