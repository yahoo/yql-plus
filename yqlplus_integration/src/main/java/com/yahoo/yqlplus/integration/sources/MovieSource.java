package com.yahoo.yqlplus.integration.sources;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.yahoo.yqlplus.api.Source;
import com.yahoo.yqlplus.api.annotations.Delete;
import com.yahoo.yqlplus.api.annotations.Insert;
import com.yahoo.yqlplus.api.annotations.Key;
import com.yahoo.yqlplus.api.annotations.Query;
import com.yahoo.yqlplus.api.annotations.Set;

import java.util.List;
import java.util.Map;

public class MovieSource implements Source {
    private final Map<String,Movie> movies = Maps.newLinkedHashMap();

    public MovieSource() {
    }


    public MovieSource(Movie movie, Movie ...movies) {
        this.movies.put(movie.getId(), movie);
        if(movies != null) {
            for (Movie m : movies) {
                this.movies.put(m.getId(), m);
            }
        }
    }


    @Query
    public synchronized List<Movie> scan() {
        return ImmutableList.copyOf(movies.values());
    }

    @Insert
    public synchronized Movie insert(@Set("$") Movie movie) {
        this.movies.put(movie.getId(), movie);
        return movie;
    }

    @Delete
    public synchronized Movie delete(@Key("id") String id) {
        return this.movies.remove(id);
    }
}
