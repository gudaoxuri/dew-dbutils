package com.ecfront.easybi.dbutils.test;

import java.math.BigDecimal;
import java.util.Date;

public class User {

    private long id;
    private String name;
    private String password;
    private int age;
    private float height1;
    private double height2;
    private Date createTime;
    private BigDecimal asset;
    private String txt;
    private boolean enable;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public BigDecimal getAsset() {
        return asset;
    }

    public void setAsset(BigDecimal asset) {
        this.asset = asset;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public float getHeight1() {
        return height1;
    }

    public void setHeight1(float height1) {
        this.height1 = height1;
    }

    public double getHeight2() {
        return height2;
    }

    public void setHeight2(double height2) {
        this.height2 = height2;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }
}
