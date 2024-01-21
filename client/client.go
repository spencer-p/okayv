package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	agent   string
	address string
	context any
	client  HTTPClient
}

func NewClient(c HTTPClient, agent, address string) *Client {
	return &Client{
		agent:   agent,
		client:  c,
		address: address,
	}
}

func (c *Client) SetAddress(address string) {
	c.address = address
}

func (c *Client) Read(key string) (string, error) {
	var body bytes.Buffer
	req := map[string]any{
		"key":            key,
		"causal-context": c.context,
	}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return "", err
	}

	httpreq, err := http.NewRequest(http.MethodGet, c.address+"/read", &body)
	if err != nil {
		return "", err
	}
	httpreq.Header.Set("User-Agent", c.agent)
	httpresp, err := c.client.Do(httpreq)
	if err != nil {
		return "", err
	}
	defer httpresp.Body.Close()
	var resp map[string]any
	if err := json.NewDecoder(httpresp.Body).Decode(&resp); err != nil {
		return "", err
	}
	if ctx, ok := resp["causal-context"]; ok {
		c.context = ctx
	}

	if httpresp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	} else if httpresp.StatusCode == http.StatusServiceUnavailable {
		return "", ErrUnavailable
	} else if httpresp.StatusCode != http.StatusOK {
		buf, err := io.ReadAll(httpresp.Body)
		errtext := string(buf)
		if err != nil {
			errtext = fmt.Sprintf("an error occurred reading the body: %s", err.Error())
		}
		return "", fmt.Errorf("read failed with code %v: %s", httpresp.StatusCode, errtext)
	}

	return resp["value"].(string), nil
}

func (c *Client) Write(key, value string) error {
	var body bytes.Buffer
	req := map[string]any{
		"key":            key,
		"value":          value,
		"causal-context": c.context,
	}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return err
	}

	httpreq, err := http.NewRequest(http.MethodPut, c.address+"/write", &body)
	if err != nil {
		return err
	}
	httpreq.Header.Set("User-Agent", c.agent)
	httpresp, err := c.client.Do(httpreq)
	if err != nil {
		return err
	}
	defer httpresp.Body.Close()

	if httpresp.StatusCode == http.StatusServiceUnavailable {
		return ErrUnavailable
	} else if httpresp.StatusCode < 200 || httpresp.StatusCode >= 300 {
		buf, err := io.ReadAll(httpresp.Body)
		errtext := string(buf)
		if err != nil {
			errtext = fmt.Sprintf("an error occurred reading the body: %s", err.Error())
		}
		return fmt.Errorf("write failed with code %v: %s", httpresp.StatusCode, errtext)
	}

	var resp map[string]any
	if err := json.NewDecoder(httpresp.Body).Decode(&resp); err != nil {
		return err
	}
	c.context = resp["causal-context"]

	return nil
}

func (c *Client) EventsWitnessed() int {
	ctx, ok := c.context.(map[string]any)
	if !ok {
		return 0
	}
	result := 0
	for _, u := range ctx {
		if v, ok := u.(float64); ok {
			result = max(result, int(v))
		}
	}
	return result
}
