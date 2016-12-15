/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.network.api;

import java.util.List;

/**
 * Represents a gateway to one or more data services.
 */
public interface DataGateway {
    DataService lookup(List<String> path);

    DataService lookupAndWatch(List<String> path, ChangeNotifier<DataService> notifier);
}
