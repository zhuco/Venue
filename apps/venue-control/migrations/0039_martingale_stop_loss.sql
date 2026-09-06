ALTER TABLE venue_support_martingale_commands DROP CONSTRAINT venue_support_martingale_commands_kind_check;
ALTER TABLE venue_support_martingale_commands ADD CONSTRAINT venue_support_martingale_commands_kind_check
    CHECK(kind IN ('entry','add','tp','cancel_tp','sl','cancel_sl_tp'));
