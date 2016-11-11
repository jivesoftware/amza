window.$ = window.jQuery;

window.sync = {};

sync.resetButton = function ($button, value) {
    $button.val(value);
    $button.removeAttr('disabled');
};

sync.statusFocus = {
    init: function () {
    },

    reset: function(ele, partition) {
        var $button = $(ele);
        $button.attr('disabled', 'disabled');
        var value = $button.val();
        $.ajax({
            type: "POST",
            url: "/amza/sync/reset/" + partition,
            data: {},
            //contentType: "application/json",
            success: function () {
                window.location.reload(true);
            },
            error: function () {
                $button.val('Failure');
                setTimeout(function () {
                    sync.resetButton($button, value);
                }, 2000);
            }
        });
    }
};

$(document).ready(function () {
    if ($.fn.dropdown) {
        $('.dropdown-toggle').dropdown();
    }

    sync.windowFocused = true;
    sync.onWindowFocus = [];
    sync.onWindowBlur = [];

    if ($('#status-focus').length) {
        sync.statusFocus.init();
    }

    $(function () {
        var hack = {};
        $('[rel="popover"]').popover({
            container: 'body',
            html: true,
            content: function () {
                console.log($(this).attr('id'));
                var h = $($(this).data('popover-content')).removeClass('hide');
                hack[$(this).attr('id')] = h;
                return h;
            }
        }).click(function (e) {
            e.preventDefault();
        }).on('hidden.bs.popover', function () {
            var h = hack[$(this).attr('id')];
            h.detach();
            h.addClass('hide');
            $('body').append(h);
        });
    });
});

$(window).focus(function () {
    sync.windowFocused = true;
    for (var i = 0; i < sync.onWindowFocus.length; i++) {
        sync.onWindowFocus[i]();
    }
}).blur(function () {
    sync.windowFocused = false;
    for (var i = 0; i < sync.onWindowBlur.length; i++) {
        sync.onWindowBlur[i]();
    }
});
