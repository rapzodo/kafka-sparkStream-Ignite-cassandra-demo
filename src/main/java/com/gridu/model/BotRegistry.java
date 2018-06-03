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
    private long categories;
    @QuerySqlField(index = true)
    private double viewsClicksDiff;
    @QuerySqlField
    private long views;
    @QuerySqlField
    private long clicks;

    public BotRegistry(){}

    public BotRegistry(String ip, long events, long categories, double viewsClicksDiff, long views, long clicks) {
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

    public long getCategories() {
        return categories;
    }

    public long getEvents() {
        return events;
    }

    public void setEvents(long events) {
        this.events = events;
    }

    public void setCategories(long categories) {
        this.categories = categories;
    }

    public double getViewsClicksDiff() {
        return viewsClicksDiff;
    }

    public void setViewsClicksDiff(double viewsClicksDiff) {
        this.viewsClicksDiff = viewsClicksDiff;
    }

    public long getViews() {
        return views;
    }

    public void setViews(long views) {
        this.views = views;
    }

    public long getClicks() {
        return clicks;
    }

    public void setClicks(long clicks) {
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
