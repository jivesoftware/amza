package com.jivesoftware.os.amzabot.deployable.ui.health;

import com.jivesoftware.os.amzabot.deployable.ui.UiRegion;

public class UiService {

    private final UiRegion<Void> homeRegion;

    public UiService(UiRegion<Void> homeRegion) {
        this.homeRegion = homeRegion;
    }

    public String render() {
        return homeRegion.render(null);
    }

}
