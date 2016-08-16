package com.jivesoftware.os.amzabot.deployable.ui.amzabot;

import com.jivesoftware.os.amzabot.deployable.ui.UiRegion;

public class AmzaBotUIService {

    private final UiRegion<AmzaBotInput> mainRegion;

    public AmzaBotUIService(UiRegion<AmzaBotInput> mainRegion) {
        this.mainRegion = mainRegion;
    }

    public String render(AmzaBotInput input) {
        return mainRegion.render(input);
    }

}
