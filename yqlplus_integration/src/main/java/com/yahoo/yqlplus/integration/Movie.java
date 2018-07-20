package com.yahoo.yqlplus.integration;

public class Movie {
    private String id;
    private String title;
    private String category;
    private String prodDate;
    private int duration;

    public Movie() {
    }

    public Movie(String id, String title, String category, String prodDate, int duration) {
        this.id = id;
        this.title = title;
        this.category = category;
        this.prodDate = prodDate;
        this.duration = duration;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getProdDate() {
        return prodDate;
    }

    public void setProdDate(String prodDate) {
        this.prodDate = prodDate;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }
}
