package main

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// RunCredentialAddTUI opens an interactive TUI for creating or editing a
// credential. It blocks until the user saves or cancels.
// Returns the completed Credential and true on save, or zero value and false
// on cancel.
func RunCredentialAddTUI(existing *Credential) (Credential, bool) {
	app := tview.NewApplication()

	// fieldRow tracks the dynamic key/value pairs the user is building.
	type fieldRow struct {
		keyInput *tview.InputField
		valInput *tview.InputField
	}

	var rows []fieldRow

	// nameInput is always present.
	nameInput := tview.NewInputField().
		SetLabel("Credential name: ").
		SetFieldWidth(30)
	if existing != nil {
		nameInput.SetText(existing.Name)
	}

	// fieldsBox holds the dynamic rows between the name and the buttons.
	fieldsBox := tview.NewFlex().SetDirection(tview.FlexRow)

	makeRow := func(key, val string) fieldRow {
		ki := tview.NewInputField().
			SetLabel("  Key:   ").
			SetFieldWidth(20).
			SetText(key)
		vi := tview.NewInputField().
			SetLabel("  Value: ").
			SetFieldWidth(40).
			SetMaskCharacter('*').
			SetText(val)
		return fieldRow{keyInput: ki, valInput: vi}
	}

	rebuildFieldsBox := func() {
		fieldsBox.Clear()
		for i, r := range rows {
			idx := i // capture
			label := tview.NewTextView().
				SetDynamicColors(true).
				SetText(fmt.Sprintf("[yellow]Field %d[-]", idx+1))
			fieldsBox.AddItem(label, 1, 0, false)
			fieldsBox.AddItem(r.keyInput, 1, 0, true)
			fieldsBox.AddItem(r.valInput, 1, 0, false)
		}
	}

	addField := func(key, val string) {
		rows = append(rows, makeRow(key, val))
		rebuildFieldsBox()
	}

	// Pre-populate from existing credential or start with one blank field.
	if existing != nil && len(existing.Fields) > 0 {
		for _, f := range existing.Fields {
			addField(f.Key, f.Value)
		}
	} else {
		addField("", "")
	}

	var saved bool
	var result Credential

	addBtn := tview.NewButton("[ Add Field ]").SetSelectedFunc(func() {
		addField("", "")
		app.SetFocus(rows[len(rows)-1].keyInput)
		app.Draw()
	})

	saveBtn := tview.NewButton("[  Save  ]").SetSelectedFunc(func() {
		name := strings.TrimSpace(nameInput.GetText())
		if name == "" {
			return
		}
		var fields []CredentialField
		for _, r := range rows {
			k := strings.TrimSpace(r.keyInput.GetText())
			v := r.valInput.GetText()
			if k != "" {
				fields = append(fields, CredentialField{Key: k, Value: v})
			}
		}
		if len(fields) == 0 {
			return
		}
		result = Credential{Name: name, Fields: fields}
		saved = true
		app.Stop()
	})

	cancelBtn := tview.NewButton("[ Cancel ]").SetSelectedFunc(func() {
		app.Stop()
	})

	buttonRow := tview.NewFlex().
		SetDirection(tview.FlexColumn).
		AddItem(addBtn, 15, 0, false).
		AddItem(tview.NewBox(), 2, 0, false).
		AddItem(saveBtn, 12, 0, false).
		AddItem(tview.NewBox(), 2, 0, false).
		AddItem(cancelBtn, 12, 0, false).
		AddItem(nil, 0, 1, false)

	instructions := tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Tab[white] moves between fields  [yellow]Enter[white] activates buttons  [yellow]Esc[white] cancels")

	layout := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(nameInput, 1, 0, true).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(fieldsBox, 0, 1, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(buttonRow, 1, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(instructions, 1, 0, false)

	frame := tview.NewFrame(layout).
		SetBorders(1, 1, 1, 1, 2, 2).
		AddText(" Add Credential ", true, tview.AlignCenter, tcell.ColorYellow)

	// Tab order: nameInput → field rows (key, val alternating) → addBtn → saveBtn → cancelBtn → back
	focusOrder := func() []tview.Primitive {
		order := []tview.Primitive{nameInput}
		for _, r := range rows {
			order = append(order, r.keyInput, r.valInput)
		}
		order = append(order, addBtn, saveBtn, cancelBtn)
		return order
	}

	currentFocus := 0
	prims := focusOrder()
	app.SetFocus(prims[0])

	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc:
			app.Stop()
			return nil
		case tcell.KeyTab:
			prims = focusOrder()
			currentFocus = (currentFocus + 1) % len(prims)
			app.SetFocus(prims[currentFocus])
			return nil
		case tcell.KeyBacktab:
			prims = focusOrder()
			currentFocus = (currentFocus + len(prims) - 1) % len(prims)
			app.SetFocus(prims[currentFocus])
			return nil
		}
		return event
	})

	app.SetRoot(frame, true)
	if err := app.Run(); err != nil {
		return Credential{}, false
	}
	return result, saved
}

// RunCredentialListTUI opens a read-only TUI showing stored credential names
// and their fields (values masked). Returns when the user presses q or Esc.
func RunCredentialListTUI(creds []Credential) {
	app := tview.NewApplication()

	text := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true)

	var sb strings.Builder
	if len(creds) == 0 {
		sb.WriteString("[yellow]No credentials stored.[white]\n\n")
		sb.WriteString("Use [green]credentials add[white] to add one.\n")
	} else {
		for _, c := range creds {
			sb.WriteString(fmt.Sprintf("[yellow]%s[white]\n", c.Name))
			for _, f := range c.Fields {
				sb.WriteString(fmt.Sprintf("  %-20s  [gray]%s[white]\n", f.Key, maskValue(f.Value)))
			}
			sb.WriteString("\n")
		}
	}
	text.SetText(sb.String())

	text.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Rune() == 'q' {
			app.Stop()
			return nil
		}
		return event
	})

	frame := tview.NewFrame(text).
		SetBorders(1, 1, 1, 1, 2, 2).
		AddText(" Stored Credentials — q to close ", true, tview.AlignCenter, tcell.ColorYellow)

	app.SetRoot(frame, true).SetFocus(text)
	app.Run() //nolint:errcheck
}

func maskValue(v string) string {
	if len(v) == 0 {
		return "(empty)"
	}
	if len(v) <= 4 {
		return strings.Repeat("*", len(v))
	}
	return v[:2] + strings.Repeat("*", len(v)-4) + v[len(v)-2:]
}
