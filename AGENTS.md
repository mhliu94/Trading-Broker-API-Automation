# Trading automation preferences

The user wants broker pre-action delay logic to remain configurable in code.
Default these delays to **0 seconds** in every new trading script, account,
example configuration, installer and scaffold. Do not introduce a fixed five-second
pre-action sleep or enable a nonzero delay unless explicitly requested.

This preference concerns artificial waits before broker actions. Preserve actual
API rate limits, order/cancellation confirmation waits, requested FOK/IOC timing,
and separately configured algorithm cycle/send intervals unless the user asks to
change those as well. Keep credentials out of source control.
