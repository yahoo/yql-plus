/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.sources;

import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Delete;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Set;
import com.yahoo.yqlplus.api.annotations.Update;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ReviewSource implements Source {
    private final AtomicInteger idSource = new AtomicInteger(1);
    public static class Review {
        private int id;
        private String title;
        private String review;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getReview() {
            return review;
        }

        public void setReview(String review) {
            this.review = review;
        }
    }

    public static class ReviewUpdate {
        private String title;
        private String review;

        public ReviewUpdate() {
        }

        public ReviewUpdate(String title, String review) {
            this.title = title;
            this.review = review;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getReview() {
            return review;
        }

        public void setReview(String review) {
            this.review = review;
        }
    }

    public ReviewSource() {
    }

    public ReviewSource(ReviewUpdate r1, ReviewUpdate ...reviews) {
        insert(r1);
        if(reviews != null) {
            for(ReviewUpdate r : reviews) {
                insert(r);
            }

        }
    }


    private final Map<Integer, Review> reviews = new LinkedHashMap<>();

    @Query
    public Iterable<Review> scan() {
        return reviews.values();
    }

    @Insert
    public Review insert(@Set("$") ReviewUpdate input) {
        Review review = new Review();
        review.setId(idSource.getAndIncrement());
        review.setReview(input.getReview());
        review.setTitle(input.getTitle());
        reviews.put(review.getId(), review);
        return review;
    }

    @Update
    public Review update(final @Key("id") int id, @Set("$") ReviewUpdate review) {
        Review old = reviews.get(id);
        if (review != null) {
            old.setTitle(review.getTitle());
            old.setReview(review.getReview());
        }
        return old;
    }

    @Update
    public Iterable<Review> updateAll(@Set("$") ReviewUpdate review) {
        this.reviews.values().stream().forEach((r) -> {
            r.setTitle(review.getTitle());
            r.setReview(review.getReview());
        });
        return new LinkedHashSet<>(reviews.values());
    }

    @Delete
    public Review delete(final @Key("uuid") int id) {
        return reviews.remove(id);
    }

    @Delete
    public Iterable<Review> deleteAll() {
        Collection<Review> c = new LinkedHashSet<>(reviews.values());
        reviews.clear();
        return c;
    }
}
