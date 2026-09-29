# Vendored pyalarmdotcomajax

`pyalarmdotcomajax/` is a vendored copy of
https://github.com/JoshChapnick/pyalarmdotcomajax @ `custom`
(which carries the MFA-cookie-jar fix).

The integration imports it relatively (`from . import pyalarmdotcomajax`),
so this copy shadows any pip-installed one and survives HA core updates.

To refresh after changing the ajax fork:

    rm -rf custom_components/alarmdotcom/pyalarmdotcomajax
    cp -r <ajax-checkout>/pyalarmdotcomajax custom_components/alarmdotcom/pyalarmdotcomajax
    find custom_components/alarmdotcom/pyalarmdotcomajax -name __pycache__ -type d -exec rm -rf {} +
