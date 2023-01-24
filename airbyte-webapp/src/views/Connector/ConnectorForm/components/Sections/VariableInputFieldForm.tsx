import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useAsync, useEffectOnce } from "react-use";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { FormGroupItem, FormObjectArrayItem } from "core/form/types";

import { useConnectorForm } from "../../connectorFormContext";
import { FormSection } from "./FormSection";

interface VariableInputFormProps {
  formField: FormObjectArrayItem;
  path: string;
  item?: unknown;
  disabled?: boolean;
  onDone: (value: unknown) => void;
  onCancel: () => void;
}

export const VariableInputFieldForm: React.FC<VariableInputFormProps> = ({
  formField,
  path,
  item,
  disabled,
  onDone,
  onCancel,
}) => {
  const { watch, setValue } = useFormContext();

  const fieldValue = watch(path);
  const { validationSchema } = useConnectorForm();

  // Copy the validation from the original field to ensure that the form has all the required values field out correctly.
  const { value: isValid } = useAsync(
    async (): Promise<boolean> => yup.reach(validationSchema, path).isValid(fieldValue),
    [fieldValue, path, validationSchema]
  );

  useEffectOnce(() => {
    const initialValue =
      item ??
      // Set initial default values when user is creating a new item
      (formField.properties as FormGroupItem).properties.reduce((acc, item) => {
        if (item._type === "formItem" && item.default) {
          // Only "formItem" types have a default value
          acc[item.fieldKey] = item.default;
        }

        return acc;
      }, {} as Record<string, unknown>);

    setValue(path, initialValue);
  });

  return (
    <>
      <ModalBody maxHeight={300}>
        <FormSection blocks={formField.properties} path={path} disabled={disabled} skipAppend />
      </ModalBody>
      <ModalFooter>
        <Button
          data-testid="cancel-button"
          variant="secondary"
          onClick={() => {
            onCancel();
          }}
        >
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button
          data-testid="done-button"
          disabled={disabled || !isValid}
          onClick={() => {
            onDone(fieldValue);
          }}
        >
          <FormattedMessage id="form.done" />
        </Button>
      </ModalFooter>
    </>
  );
};
