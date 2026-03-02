package tap

import (
	"context"
)

// RunTap starts entire tap service including crawler, firehose consumer, resyncer, outbox and http server.
func (t *Tap) RunTap(ctx context.Context, addr string) error {
	svcErr := make(chan error, 1)

	if !t.outboxOnly {
		go t.Crawler.Run(ctx)

		go func() {
			t.logger.Info("starting firehose consumer")
			if err := t.Firehose.Run(ctx); err != nil {
				svcErr <- err
			}
		}()
	}

	go t.events.LoadEvents(ctx)

	if !t.outboxOnly {
		go t.Resyncer.run(ctx)
	}

	go t.outbox.Run(ctx)

	go func() {
		t.logger.Info("starting HTTP server", "addr", addr)
		if err := t.Server.Start(addr); err != nil {
			svcErr <- err
		}
	}()

	return <-svcErr
}

func (t *Tap) Shutdown(ctx context.Context) error {
	if err := t.Server.Shutdown(ctx); err != nil {
		t.logger.Error("error during shutdown", "error", err)
		return err
	}

	if err := t.CloseDb(ctx); err != nil {
		return err
	}

	return nil
}
