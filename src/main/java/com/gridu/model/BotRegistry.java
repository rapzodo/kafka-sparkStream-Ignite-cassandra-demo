package com.gridu.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Objects;

public class BotRegistry implements Serializable {
    @QuerySqlField(index = true)
    private String ip;
    @QuerySqlField(index = true)
    private long events;
    @QuerySqlField(index = true)
    private int categories;
    @QuerySqlField(index = true)
    private int viewsClicksDiff;
    private int views;
    private int clicks;

    public BotRegistry(){}

    public BotRegistry(String ip, long events, int categories, int viewsClicksDiff, int views, int clicks) {
        this.ip = ip;
        this.events = events;
        this.categories = categories;
        this.viewsClicksDiff = viewsClicksDiff;
        this.views = views;
        this.clicks = clicks;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getCategories() {
        return categories;
    }

    public long getEvents() {
        return events;
    }

    public void setEvents(long events) {
        this.events = events;
    }

    public void setCategories(int categories) {
        this.categories = categories;
    }

    public int getViewsClicksDiff() {
        return viewsClicksDiff;
    }

    public void setViewsClicksDiff(int viewsClicksDiff) {
        this.viewsClicksDiff = viewsClicksDiff;
    }

    public int getViews() {
        return views;
    }

    public void setViews(int views) {
        this.views = views;
    }

    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BotRegistry)) return false;
        BotRegistry that = (BotRegistry) o;
        return getEvents() == that.getEvents() &&
                getCategories() == that.getCategories() &&
                getViewsClicksDiff() == that.getViewsClicksDiff() &&
                getViews() == that.getViews() &&
                getClicks() == that.getClicks() &&
                Objects.equals(getIp(), that.getIp());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getIp(), getEvents(), getCategories(), getViewsClicksDiff(), getViews(), getClicks());
    }
}
