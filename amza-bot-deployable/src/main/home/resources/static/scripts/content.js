window.$ = window.jQuery;

window.miru = {};

miru.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

miru.content = {
    init: function () {
    }
};

$(document).ready(function () {
    if ($('#content').length) {
        miru.content.init();
    }
});
